package main

// Reads and writes the Cloud Firestore export format that the Java emulator's :export and
// :import endpoints use, so saved workspace namespaces and seed bundles move between the two:
//
//	<dir>/<name>/<name>.overall_export_metadata           LevelDB log: "3", then the kind index
//	<dir>/<name>/all_namespaces/all_kinds/
//	    all_namespaces_all_kinds.export_metadata           output file list
//	    output-0                                           LevelDB log of App Engine EntityProtos
//
// Each document is an EntityProto whose fields are raw properties: arrays are repeated properties
// marked multiple, maps are nested EntityProtos (meaning 19), timestamps are microseconds
// (meaning 7), bytes are strings with meaning 14, geopoints have meaning 9 and an empty array is
// a single indexed property with meaning 24.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	kindMetadataPath = "all_namespaces/all_kinds/all_namespaces_all_kinds.export_metadata"
	outputName       = "output-0"

	meaningTimestamp  = 7
	meaningGeoPoint   = 9
	meaningBlob       = 14
	meaningEntity     = 19
	meaningEmptyArray = 24
)

// --- LevelDB log framing ---

const (
	logBlockSize  = 32768
	logHeaderSize = 7
	recordFull    = 1
	recordFirst   = 2
	recordMiddle  = 3
	recordLast    = 4
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func maskedCRC(recordType byte, data []byte) uint32 {
	c := crc32.Update(crc32.Checksum([]byte{recordType}, castagnoli), castagnoli, data)
	return ((c >> 15) | (c << 17)) + 0xa282ead8
}

type logWriter struct {
	buf    bytes.Buffer
	offset int // position within the current block
}

func (w *logWriter) add(payload []byte) {
	first := true
	for {
		left := logBlockSize - w.offset
		if left < logHeaderSize {
			w.buf.Write(make([]byte, left))
			w.offset = 0
			left = logBlockSize
		}
		n := min(len(payload), left-logHeaderSize)
		end := n == len(payload)
		var recordType byte
		switch {
		case first && end:
			recordType = recordFull
		case first:
			recordType = recordFirst
		case end:
			recordType = recordLast
		default:
			recordType = recordMiddle
		}
		var header [logHeaderSize]byte
		binary.LittleEndian.PutUint32(header[:4], maskedCRC(recordType, payload[:n]))
		binary.LittleEndian.PutUint16(header[4:6], uint16(n))
		header[6] = recordType
		w.buf.Write(header[:])
		w.buf.Write(payload[:n])
		w.offset += logHeaderSize + n
		payload = payload[n:]
		first = false
		if end {
			return
		}
	}
}

func readLog(data []byte) ([][]byte, error) {
	var records [][]byte
	var pending []byte
	for i := 0; i < len(data); {
		left := logBlockSize - i%logBlockSize
		if left < logHeaderSize {
			i += left
			continue
		}
		if i+logHeaderSize > len(data) {
			break
		}
		crc := binary.LittleEndian.Uint32(data[i:])
		n := int(binary.LittleEndian.Uint16(data[i+4:]))
		recordType := data[i+6]
		i += logHeaderSize
		if recordType == 0 && n == 0 {
			i += left - logHeaderSize // zero padding to the end of the block
			continue
		}
		if i+n > len(data) {
			return nil, fmt.Errorf("truncated export record")
		}
		payload := data[i : i+n]
		i += n
		if maskedCRC(recordType, payload) != crc {
			return nil, fmt.Errorf("export record checksum mismatch")
		}
		switch recordType {
		case recordFull:
			records = append(records, payload)
		case recordFirst:
			pending = append([]byte(nil), payload...)
		case recordMiddle:
			pending = append(pending, payload...)
		case recordLast:
			records = append(records, append(pending, payload...))
			pending = nil
		default:
			return nil, fmt.Errorf("unknown export record type %d", recordType)
		}
	}
	return records, nil
}

// --- protobuf wire helpers ---

type wireField struct {
	num   protowire.Number
	typ   protowire.Type
	bytes []byte // length-delimited value or group body
	value uint64 // varint or fixed value
}

func parseWire(b []byte) ([]wireField, error) {
	var out []wireField
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		b = b[n:]
		f := wireField{num: num, typ: typ}
		switch typ {
		case protowire.VarintType:
			f.value, n = protowire.ConsumeVarint(b)
		case protowire.Fixed64Type:
			f.value, n = protowire.ConsumeFixed64(b)
		case protowire.Fixed32Type:
			var v uint32
			v, n = protowire.ConsumeFixed32(b)
			f.value = uint64(v)
		case protowire.BytesType:
			f.bytes, n = protowire.ConsumeBytes(b)
		case protowire.StartGroupType:
			f.bytes, n = protowire.ConsumeGroup(num, b)
		default:
			return nil, fmt.Errorf("unsupported wire type %d", typ)
		}
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		b = b[n:]
		out = append(out, f)
	}
	return out, nil
}

func appendString(b []byte, num protowire.Number, s string) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, s)
}

func appendBytes(b []byte, num protowire.Number, v []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

func appendVarint(b []byte, num protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func appendDouble(b []byte, num protowire.Number, v float64) []byte {
	b = protowire.AppendTag(b, num, protowire.Fixed64Type)
	return protowire.AppendFixed64(b, math.Float64bits(v))
}

func appendGroup(b []byte, num protowire.Number, body []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.StartGroupType)
	b = append(b, body...)
	return protowire.AppendTag(b, num, protowire.EndGroupType)
}

// --- EntityProto encoding ---

func pathElements(path string) []string { return strings.Split(path, "/") }

// encodePath writes a Path message: one Element group (type=2, name=4) per collection/id pair.
func encodePath(segments []string) []byte {
	var b []byte
	for i := 0; i+1 < len(segments); i += 2 {
		var element []byte
		element = appendString(element, 2, segments[i])
		element = appendString(element, 4, segments[i+1])
		b = appendGroup(b, 1, element)
	}
	return b
}

func encodeEntity(app string, segments []string, fields map[string]*pb.Value) []byte {
	var key []byte
	key = appendString(key, 13, app)
	key = appendBytes(key, 14, encodePath(segments))
	b := appendBytes(nil, 13, key)
	names := sortedKeys(fields)
	for _, name := range names {
		b = appendProperties(b, name, fields[name])
	}
	var group []string
	if len(segments) >= 2 {
		group = segments[:2]
	}
	return appendBytes(b, 16, encodePath(group))
}

func encodeProperty(meaning uint64, name string, multiple bool, value []byte) []byte {
	var p []byte
	if meaning != 0 {
		p = appendVarint(p, 1, meaning)
	}
	p = appendString(p, 3, name)
	p = appendVarint(p, 4, uint64(boolInt(multiple)))
	return appendBytes(p, 5, value)
}

func appendProperties(b []byte, name string, v *pb.Value) []byte {
	if a, ok := v.GetValueType().(*pb.Value_ArrayValue); ok {
		values := a.ArrayValue.GetValues()
		if len(values) == 0 {
			return appendBytes(b, 14, encodeProperty(meaningEmptyArray, name, false, nil))
		}
		for _, element := range values {
			meaning, value := encodeValue(element)
			b = appendBytes(b, 15, encodeProperty(meaning, name, true, value))
		}
		return b
	}
	meaning, value := encodeValue(v)
	return appendBytes(b, 15, encodeProperty(meaning, name, false, value))
}

func encodeValue(v *pb.Value) (uint64, []byte) {
	var b []byte
	switch t := v.GetValueType().(type) {
	case *pb.Value_BooleanValue:
		return 0, appendVarint(b, 2, uint64(boolInt(t.BooleanValue)))
	case *pb.Value_IntegerValue:
		return 0, appendVarint(b, 1, uint64(t.IntegerValue))
	case *pb.Value_DoubleValue:
		return 0, appendDouble(b, 4, t.DoubleValue)
	case *pb.Value_TimestampValue:
		return meaningTimestamp, appendVarint(b, 1, uint64(t.TimestampValue.AsTime().UnixMicro()))
	case *pb.Value_StringValue:
		return 0, appendString(b, 3, t.StringValue)
	case *pb.Value_BytesValue:
		return meaningBlob, appendBytes(b, 3, t.BytesValue)
	case *pb.Value_ReferenceValue:
		return 0, appendGroup(b, 12, encodeReference(t.ReferenceValue))
	case *pb.Value_GeoPointValue:
		var point []byte
		point = appendDouble(point, 6, t.GeoPointValue.GetLatitude())
		point = appendDouble(point, 7, t.GeoPointValue.GetLongitude())
		return meaningGeoPoint, appendGroup(b, 5, point)
	case *pb.Value_MapValue:
		return meaningEntity, appendBytes(b, 3, encodeEntity("", nil, t.MapValue.GetFields()))
	}
	return 0, nil // null is an empty PropertyValue
}

func encodeReference(name string) []byte {
	db, path, err := parseName(name)
	if err != nil {
		return nil
	}
	project := strings.Split(db, "/")[1]
	b := appendString(nil, 13, "dev~"+project)
	segments := pathElements(path)
	for i := 0; i+1 < len(segments); i += 2 {
		var element []byte
		element = appendString(element, 15, segments[i])
		element = appendString(element, 17, segments[i+1])
		b = appendGroup(b, 14, element)
	}
	return b
}

// --- EntityProto decoding ---

type decodedEntity struct {
	segments []string
	fields   map[string]*pb.Value
}

func decodeEntity(b []byte) (*decodedEntity, error) {
	fields, err := parseWire(b)
	if err != nil {
		return nil, err
	}
	e := &decodedEntity{fields: map[string]*pb.Value{}}
	for _, f := range fields {
		switch f.num {
		case 13:
			key, err := parseWire(f.bytes)
			if err != nil {
				return nil, err
			}
			for _, k := range key {
				if k.num == 14 {
					if e.segments, err = decodePath(k.bytes); err != nil {
						return nil, err
					}
				}
			}
		case 14, 15:
			if err := decodeProperty(f.bytes, e.fields); err != nil {
				return nil, err
			}
		}
	}
	return e, nil
}

func decodePath(b []byte) ([]string, error) {
	elements, err := parseWire(b)
	if err != nil {
		return nil, err
	}
	var segments []string
	for _, element := range elements {
		if element.num != 1 {
			continue
		}
		kind, id, err := decodeElement(element.bytes, 2, 3, 4)
		if err != nil {
			return nil, err
		}
		segments = append(segments, kind, id)
	}
	return segments, nil
}

// decodeElement reads a path element's kind and its name, or its numeric id as __id<n>__.
func decodeElement(b []byte, kindField, idField, nameField protowire.Number) (string, string, error) {
	fields, err := parseWire(b)
	if err != nil {
		return "", "", err
	}
	var kind, id string
	for _, f := range fields {
		switch f.num {
		case kindField:
			kind = string(f.bytes)
		case nameField:
			id = string(f.bytes)
		case idField:
			id = "__id" + strconv.FormatInt(int64(f.value), 10) + "__"
		}
	}
	return kind, id, nil
}

func decodeProperty(b []byte, into map[string]*pb.Value) error {
	fields, err := parseWire(b)
	if err != nil {
		return err
	}
	var meaning uint64
	var name string
	var multiple bool
	var value []byte
	for _, f := range fields {
		switch f.num {
		case 1:
			meaning = f.value
		case 3:
			name = string(f.bytes)
		case 4:
			multiple = f.value != 0
		case 5:
			value = f.bytes
		}
	}
	if meaning == meaningEmptyArray {
		into[name] = arrayValue(nil)
		return nil
	}
	v, err := decodeValue(meaning, value)
	if err != nil {
		return err
	}
	if !multiple {
		into[name] = v
		return nil
	}
	existing, ok := into[name].GetValueType().(*pb.Value_ArrayValue)
	if !ok {
		existing = &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{}}
		into[name] = &pb.Value{ValueType: existing}
	}
	existing.ArrayValue.Values = append(existing.ArrayValue.Values, v)
	return nil
}

func decodeValue(meaning uint64, b []byte) (*pb.Value, error) {
	fields, err := parseWire(b)
	if err != nil {
		return nil, err
	}
	for _, f := range fields {
		switch f.num {
		case 1:
			if meaning == meaningTimestamp {
				return &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: micros(int64(f.value))}}, nil
			}
			return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: int64(f.value)}}, nil
		case 2:
			return &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: f.value != 0}}, nil
		case 3:
			switch meaning {
			case meaningBlob:
				return &pb.Value{ValueType: &pb.Value_BytesValue{BytesValue: append([]byte{}, f.bytes...)}}, nil
			case meaningEntity:
				nested, err := decodeEntity(f.bytes)
				if err != nil {
					return nil, err
				}
				return &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: nested.fields}}}, nil
			}
			return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: string(f.bytes)}}, nil
		case 4:
			return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: math.Float64frombits(f.value)}}, nil
		case 5:
			point, err := parseWire(f.bytes)
			if err != nil {
				return nil, err
			}
			geo := &latlng.LatLng{}
			for _, p := range point {
				switch p.num {
				case 6:
					geo.Latitude = math.Float64frombits(p.value)
				case 7:
					geo.Longitude = math.Float64frombits(p.value)
				}
			}
			return &pb.Value{ValueType: &pb.Value_GeoPointValue{GeoPointValue: geo}}, nil
		case 12:
			name, err := decodeReference(f.bytes)
			if err != nil {
				return nil, err
			}
			return &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: name}}, nil
		}
	}
	return nullValue(), nil
}

func decodeReference(b []byte) (string, error) {
	fields, err := parseWire(b)
	if err != nil {
		return "", err
	}
	project := ""
	var segments []string
	for _, f := range fields {
		switch f.num {
		case 13:
			project = string(f.bytes)
			if i := strings.IndexByte(project, '~'); i >= 0 {
				project = project[i+1:]
			}
		case 14:
			kind, id, err := decodeElement(f.bytes, 15, 16, 17)
			if err != nil {
				return "", err
			}
			segments = append(segments, kind, id)
		}
	}
	return "projects/" + project + "/databases/(default)/documents/" + strings.Join(segments, "/"), nil
}

// --- export and import ---

func (s *Store) exportDatabase(dbName, directory, name string) error {
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		return errorf(codes.FailedPrecondition, "export_directory must be a directory")
	}
	if name == "" {
		name = time.Now().UTC().Format("2006-01-02T15:04:05_") + strconv.Itoa(time.Now().Nanosecond()/1000)
	}
	db := s.database(dbName)
	project := strings.Split(dbName, "/")[1]
	start := time.Now().UnixMicro()

	db.mu.RLock()
	paths := make([]string, 0, len(db.docs))
	for path := range db.docs {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var output logWriter
	for _, path := range paths {
		output.add(encodeEntity("dev~"+project, pathElements(path), db.docs[path].fields()))
	}
	db.mu.RUnlock()

	root := filepath.Join(directory, name)
	if err := os.MkdirAll(filepath.Join(root, filepath.Dir(kindMetadataPath)), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, filepath.Dir(kindMetadataPath), outputName), output.buf.Bytes(), 0o644); err != nil {
		return err
	}
	var range_, files, kind []byte
	range_ = appendString(range_, 1, project)
	range_ = appendVarint(range_, 2, uint64(start))
	range_ = appendVarint(range_, 3, uint64(time.Now().UnixMicro()))
	files = appendBytes(files, 1, nil)
	files = appendString(files, 2, outputName)
	kind = appendBytes(kind, 1, range_)
	kind = appendBytes(kind, 2, files)
	if err := os.WriteFile(filepath.Join(root, kindMetadataPath), kind, 0o644); err != nil {
		return err
	}
	var version, index, overall []byte
	version = appendVarint(version, 1, 2)
	version = appendVarint(version, 3, 3)
	index = appendBytes(index, 1, version)
	index = appendString(index, 2, kindMetadataPath)
	index = appendVarint(index, 3, uint64(len(paths)))
	index = appendVarint(index, 4, uint64(output.buf.Len()))
	overall = appendBytes(overall, 1, index)
	var metadata logWriter
	metadata.add([]byte("3"))
	metadata.add(overall)
	return os.WriteFile(filepath.Join(root, name+".overall_export_metadata"), metadata.buf.Bytes(), 0o644)
}

// importDatabase upserts every document from an export. source is the .overall_export_metadata
// file, or a directory containing exactly one.
func (s *Store) importDatabase(dbName, source string) (int, error) {
	if info, err := os.Stat(source); err == nil && info.IsDir() {
		matches, _ := filepath.Glob(filepath.Join(source, "*.overall_export_metadata"))
		if len(matches) != 1 {
			return 0, fmt.Errorf("%s does not contain exactly one .overall_export_metadata file", source)
		}
		source = matches[0]
	}
	outputs, err := exportOutputs(source)
	if err != nil {
		return 0, err
	}
	var entities []*decodedEntity
	for _, output := range outputs {
		data, err := os.ReadFile(output)
		if err != nil {
			return 0, err
		}
		records, err := readLog(data)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", output, err)
		}
		for _, record := range records {
			e, err := decodeEntity(record)
			if err != nil {
				return 0, fmt.Errorf("%s: %w", output, err)
			}
			if len(e.segments) == 0 || len(e.segments)%2 != 0 {
				return 0, fmt.Errorf("%s: entity without a document key", output)
			}
			entities = append(entities, e)
		}
	}
	db := s.database(dbName)
	now := s.now()
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, e := range entities {
		db.put(&entry{path: strings.Join(e.segments, "/"), data: marshalFields(e.fields), create: now, update: now})
	}
	return len(entities), nil
}

// exportOutputs follows the overall metadata to the kind metadata files and their outputs.
func exportOutputs(metadataFile string) ([]string, error) {
	data, err := os.ReadFile(metadataFile)
	if err != nil {
		return nil, err
	}
	records, err := readLog(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", metadataFile, err)
	}
	// Java exports an empty database as version 3 followed by an empty
	// index record, with no kind metadata or output files. This is a valid
	// namespace checkpoint; a truncated or otherwise missing index is not.
	if len(records) == 2 && bytes.Equal(records[0], []byte("3")) && len(records[1]) == 0 {
		return nil, nil
	}
	root := filepath.Dir(metadataFile)
	var outputs []string
	for _, record := range records {
		fields, err := parseWire(record)
		if err != nil {
			continue // the leading version record is not a message
		}
		for _, f := range fields {
			if f.num != 1 || f.typ != protowire.BytesType {
				continue
			}
			index, err := parseWire(f.bytes)
			if err != nil {
				return nil, err
			}
			for _, i := range index {
				if i.num != 2 || i.typ != protowire.BytesType {
					continue
				}
				kindFile := filepath.Join(root, string(i.bytes))
				kindData, err := os.ReadFile(kindFile)
				if err != nil {
					return nil, err
				}
				kind, err := parseWire(kindData)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", kindFile, err)
				}
				for _, k := range kind {
					if k.num != 2 {
						continue
					}
					entries, err := parseWire(k.bytes)
					if err != nil {
						return nil, err
					}
					for _, e := range entries {
						if e.num == 2 {
							outputs = append(outputs, filepath.Join(filepath.Dir(kindFile), string(e.bytes)))
						}
					}
				}
			}
		}
	}
	if len(outputs) == 0 {
		return nil, fmt.Errorf("%s lists no export output files", metadataFile)
	}
	return outputs, nil
}
