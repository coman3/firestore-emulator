package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A document keeps only its current version, as deterministic protobuf bytes. Unlike the Java
// emulator nothing is retained per write, so memory tracks live data rather than write volume.
type entry struct {
	path   string // relative to the database's documents root, e.g. "staff/abc/notes/n1"
	data   []byte // marshalled pb.MapValue
	create int64  // unix microseconds
	update int64
}

func (e *entry) fields() map[string]*pb.Value {
	var m pb.MapValue
	if err := proto.Unmarshal(e.data, &m); err != nil {
		panic(err) // data is only ever written by marshalFields
	}
	if m.Fields == nil {
		m.Fields = map[string]*pb.Value{}
	}
	return m.Fields
}

var deterministic = proto.MarshalOptions{Deterministic: true}

func marshalFields(fields map[string]*pb.Value) []byte {
	data, err := deterministic.Marshal(&pb.MapValue{Fields: fields})
	if err != nil {
		panic(err)
	}
	return data
}

type database struct {
	name string // projects/{project}/databases/{database}
	mu   sync.RWMutex
	docs map[string]*entry
	// collection path ("staff" or "staff/abc/notes") -> document id -> entry
	collections map[string]map[string]*entry
}

func (db *database) root() string { return db.name + "/documents" }

func (db *database) documentName(path string) string { return db.root() + "/" + path }

func (db *database) put(e *entry) {
	db.docs[e.path] = e
	parent, id := splitPath(e.path)
	if db.collections[parent] == nil {
		db.collections[parent] = map[string]*entry{}
	}
	db.collections[parent][id] = e
}

func (db *database) remove(path string) {
	delete(db.docs, path)
	parent, id := splitPath(path)
	if c := db.collections[parent]; c != nil {
		delete(c, id)
		if len(c) == 0 {
			delete(db.collections, parent)
		}
	}
}

func (db *database) clear() {
	db.docs = map[string]*entry{}
	db.collections = map[string]map[string]*entry{}
}

// splitPath separates "a/b/c/d" into the collection path "a/b/c" and the id "d".
func splitPath(path string) (string, string) {
	i := strings.LastIndexByte(path, '/')
	return path[:i], path[i+1:]
}

type transaction struct {
	db       string
	readOnly bool
	reads    map[string]int64 // document path -> update time when read (0 when missing)
	queries  []recordedQuery
}

// A query a transaction ran and the documents it returned, so a commit also notices a concurrent write that changed
// the result set (an insert into it, or a delete from it), not just a change to a document it returned.
type recordedQuery struct {
	parent string
	query  *pb.StructuredQuery
	result string
}

func resultKey(rows []*row) string {
	paths := make([]string, len(rows))
	for i, r := range rows {
		paths[i] = r.e.path
	}
	sort.Strings(paths)
	return strings.Join(paths, "\n")
}

type Store struct {
	mu   sync.Mutex
	dbs  map[string]*database
	txs  map[string]*transaction
	last int64 // last commit time handed out, unix microseconds
}

func NewStore() *Store {
	return &Store{dbs: map[string]*database{}, txs: map[string]*transaction{}}
}

// database returns the database for a projects/{p}/databases/{d} name, creating it on first use,
// so any number of projects and databases share one process.
func (s *Store) database(name string) *database {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := s.dbs[name]
	if db == nil {
		db = &database{name: name}
		db.clear()
		s.dbs[name] = db
	}
	return db
}

// now returns a strictly increasing commit time with microsecond precision.
func (s *Store) now() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := time.Now().UnixMicro()
	if t <= s.last {
		t = s.last + 1
	}
	s.last = t
	return t
}

func micros(t int64) *timestamppb.Timestamp { return timestamppb.New(time.UnixMicro(t)) }

// resource splits a full document or root name into its database and relative path.
func parseName(name string) (db string, path string, err error) {
	parts := strings.SplitN(name, "/", 6)
	if len(parts) < 5 || parts[0] != "projects" || parts[2] != "databases" || parts[4] != "documents" || parts[1] == "" || parts[3] == "" {
		return "", "", status.Errorf(codes.InvalidArgument, "invalid resource name %q", name)
	}
	db = strings.Join(parts[:4], "/")
	if len(parts) == 6 {
		path = strings.Trim(parts[5], "/")
	}
	return db, path, nil
}

func parseDocumentName(name string) (string, string, error) {
	db, path, err := parseName(name)
	if err == nil && (path == "" || strings.Count(path, "/")%2 == 0) {
		err = status.Errorf(codes.InvalidArgument, "%q is not a document name", name)
	}
	return db, path, err
}

func newID(size int) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

func (s *Store) beginTransaction(db string, options *pb.TransactionOptions) []byte {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txs[hex.EncodeToString(id)] = &transaction{db: db, readOnly: options.GetReadOnly() != nil, reads: map[string]int64{}}
	return id
}

func (s *Store) transaction(db string, id []byte) (*transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := s.txs[hex.EncodeToString(id)]
	if tx == nil || tx.db != db {
		return nil, status.Error(codes.InvalidArgument, "transaction is invalid or has expired")
	}
	return tx, nil
}

func (s *Store) endTransaction(id []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.txs, hex.EncodeToString(id))
}

// recordRead remembers what a transaction saw, so its commit can detect a concurrent change.
func (s *Store) recordRead(tx *transaction, path string, e *entry) {
	if tx == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, seen := tx.reads[path]; !seen {
		tx.reads[path] = updateTime(e)
	}
}

func (s *Store) recordQuery(tx *transaction, parent string, query *pb.StructuredQuery, rows []*row) {
	if tx == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx.queries = append(tx.queries, recordedQuery{parent: parent, query: query, result: resultKey(rows)})
}

func updateTime(e *entry) int64 {
	if e == nil {
		return 0
	}
	return e.update
}

// commit applies writes atomically. Within a transaction it aborts if anything the transaction
// read has changed since, which the client library answers by retrying the transaction.
func (s *Store) commit(dbName string, writes []*pb.Write, txID []byte) (*pb.CommitResponse, error) {
	db := s.database(dbName)
	var tx *transaction
	if len(txID) > 0 {
		var err error
		if tx, err = s.transaction(dbName, txID); err != nil {
			return nil, err
		}
		defer s.endTransaction(txID)
		if tx.readOnly && len(writes) > 0 {
			return nil, status.Error(codes.InvalidArgument, "cannot write in a read-only transaction")
		}
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if tx != nil {
		s.mu.Lock()
		for path, seen := range tx.reads {
			if updateTime(db.docs[path]) != seen {
				s.mu.Unlock()
				return nil, status.Error(codes.Aborted, "transaction aborted because a document it read was modified concurrently")
			}
		}
		queries := tx.queries
		s.mu.Unlock()
		for _, q := range queries {
			if rows, err := db.runQuery(q.parent, q.query); err != nil || resultKey(rows) != q.result {
				return nil, status.Error(codes.Aborted, "transaction aborted because the results of a query it ran changed concurrently")
			}
		}
	}
	now := s.now()
	staged := map[string]*entry{}
	deleted := map[string]bool{}
	current := func(path string) *entry {
		if e, ok := staged[path]; ok {
			return e
		}
		if deleted[path] {
			return nil
		}
		return db.docs[path]
	}
	results := make([]*pb.WriteResult, 0, len(writes))
	for _, w := range writes {
		result, path, next, err := applyWrite(dbName, w, current, now)
		if err != nil {
			return nil, err
		}
		if next == nil {
			delete(staged, path)
			deleted[path] = true
		} else {
			staged[path] = next
			delete(deleted, path)
		}
		results = append(results, result)
	}
	for path := range deleted {
		db.remove(path)
	}
	for _, e := range staged {
		db.put(e)
	}
	return &pb.CommitResponse{WriteResults: results, CommitTime: micros(now)}, nil
}

// batchWrite applies each write on its own, reporting a status per write.
func (s *Store) batchWrite(dbName string, writes []*pb.Write) *pb.BatchWriteResponse {
	response := &pb.BatchWriteResponse{}
	for _, w := range writes {
		result, err := s.commit(dbName, []*pb.Write{w}, nil)
		st, _ := status.FromError(err)
		response.Status = append(response.Status, st.Proto())
		if err != nil {
			response.WriteResults = append(response.WriteResults, &pb.WriteResult{})
		} else {
			response.WriteResults = append(response.WriteResults, result.WriteResults[0])
		}
	}
	return response
}

func applyWrite(dbName string, w *pb.Write, current func(string) *entry, now int64) (*pb.WriteResult, string, *entry, error) {
	var name string
	switch op := w.GetOperation().(type) {
	case *pb.Write_Update:
		name = op.Update.GetName()
	case *pb.Write_Delete:
		name = op.Delete
	case *pb.Write_Transform:
		name = op.Transform.GetDocument()
	default:
		return nil, "", nil, status.Error(codes.InvalidArgument, "write has no operation")
	}
	db, path, err := parseDocumentName(name)
	if err != nil {
		return nil, "", nil, err
	}
	if db != dbName {
		return nil, "", nil, status.Errorf(codes.InvalidArgument, "%q is not in database %q", name, dbName)
	}
	old := current(path)
	if err := checkPrecondition(name, w.GetCurrentDocument(), old); err != nil {
		return nil, "", nil, err
	}
	switch op := w.GetOperation().(type) {
	case *pb.Write_Delete:
		return &pb.WriteResult{UpdateTime: micros(now)}, path, nil, nil
	case *pb.Write_Transform:
		if old == nil {
			return nil, "", nil, status.Errorf(codes.NotFound, "No document to update: %s", name)
		}
		return transformDocument(path, old, old.fields(), op.Transform.GetFieldTransforms(), now)
	}
	update := w.GetUpdate()
	fields := map[string]*pb.Value{}
	if mask := w.GetUpdateMask(); mask != nil {
		if old != nil {
			fields = old.fields()
		}
		for _, p := range mask.GetFieldPaths() {
			segments, err := parseFieldPath(p)
			if err != nil {
				return nil, "", nil, status.Error(codes.InvalidArgument, err.Error())
			}
			if v, ok := getField(update.GetFields(), segments); ok {
				setField(fields, segments, proto.Clone(v).(*pb.Value))
			} else {
				deleteField(fields, segments)
			}
		}
	} else {
		for k, v := range update.GetFields() {
			fields[k] = proto.Clone(v).(*pb.Value)
		}
	}
	for _, v := range fields {
		truncateTimestamps(v)
	}
	return transformDocument(path, old, fields, w.GetUpdateTransforms(), now)
}

func checkPrecondition(name string, pre *pb.Precondition, old *entry) error {
	switch c := pre.GetConditionType().(type) {
	case *pb.Precondition_Exists:
		if c.Exists && old == nil {
			return status.Errorf(codes.NotFound, "No document to update: %s", name)
		}
		if !c.Exists && old != nil {
			return status.Errorf(codes.AlreadyExists, "Document already exists: %s", name)
		}
	case *pb.Precondition_UpdateTime:
		want := c.UpdateTime.AsTime().UnixMicro()
		if old == nil || old.update != want {
			return status.Errorf(codes.FailedPrecondition, "the stored version of %s does not match the required update time", name)
		}
	}
	return nil
}

// transformDocument applies field transforms, then keeps the previous update time when the write
// changed nothing, as Firestore does.
func transformDocument(path string, old *entry, fields map[string]*pb.Value, transforms []*pb.DocumentTransform_FieldTransform, now int64) (*pb.WriteResult, string, *entry, error) {
	result := &pb.WriteResult{}
	for _, t := range transforms {
		segments, err := parseFieldPath(t.GetFieldPath())
		if err != nil {
			return nil, "", nil, status.Error(codes.InvalidArgument, err.Error())
		}
		previous, _ := getField(fields, segments)
		next, out := applyTransform(previous, t, now)
		setField(fields, segments, next)
		result.TransformResults = append(result.TransformResults, out)
	}
	data := marshalFields(fields)
	next := &entry{path: path, data: data, create: now, update: now}
	if old != nil {
		next.create = old.create
		if bytes.Equal(old.data, data) {
			next.update = old.update
		}
	}
	result.UpdateTime = micros(next.update)
	return result, path, next, nil
}

func applyTransform(previous *pb.Value, t *pb.DocumentTransform_FieldTransform, now int64) (*pb.Value, *pb.Value) {
	switch op := t.GetTransformType().(type) {
	case *pb.DocumentTransform_FieldTransform_SetToServerValue:
		v := &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: micros(now)}}
		return v, v
	case *pb.DocumentTransform_FieldTransform_Increment:
		v := add(previous, op.Increment)
		return v, v
	case *pb.DocumentTransform_FieldTransform_Maximum:
		v := op.Maximum
		if previous != nil && isNumber(previous) && compareNumbers(previous, op.Maximum) >= 0 {
			v = previous
		}
		return v, v
	case *pb.DocumentTransform_FieldTransform_Minimum:
		v := op.Minimum
		if previous != nil && isNumber(previous) && compareNumbers(previous, op.Minimum) <= 0 {
			v = previous
		}
		return v, v
	case *pb.DocumentTransform_FieldTransform_AppendMissingElements:
		values := existingArray(previous)
		for _, candidate := range op.AppendMissingElements.GetValues() {
			if !containsValue(values, candidate) {
				values = append(values, candidate)
			}
		}
		return arrayValue(values), nullValue()
	case *pb.DocumentTransform_FieldTransform_RemoveAllFromArray:
		var kept []*pb.Value
		for _, v := range existingArray(previous) {
			if !containsValue(op.RemoveAllFromArray.GetValues(), v) {
				kept = append(kept, v)
			}
		}
		return arrayValue(kept), nullValue()
	}
	return previous, nullValue()
}

func existingArray(v *pb.Value) []*pb.Value {
	if a, ok := v.GetValueType().(*pb.Value_ArrayValue); ok {
		return append([]*pb.Value(nil), a.ArrayValue.GetValues()...)
	}
	return nil
}

func arrayValue(values []*pb.Value) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: values}}}
}

func containsValue(values []*pb.Value, v *pb.Value) bool {
	for _, candidate := range values {
		if valuesEqual(candidate, v) {
			return true
		}
	}
	return false
}

// add implements the increment transform: integer arithmetic saturates, any double makes a double,
// and a missing or non-numeric field is replaced by the operand.
func add(previous, operand *pb.Value) *pb.Value {
	if previous == nil || !isNumber(previous) {
		return operand
	}
	a, aInt := previous.GetValueType().(*pb.Value_IntegerValue)
	b, bInt := operand.GetValueType().(*pb.Value_IntegerValue)
	if aInt && bInt {
		sum := a.IntegerValue + b.IntegerValue
		switch {
		case b.IntegerValue > 0 && sum < a.IntegerValue:
			sum = math.MaxInt64
		case b.IntegerValue < 0 && sum > a.IntegerValue:
			sum = math.MinInt64
		}
		return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: sum}}
	}
	return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: asDouble(previous) + asDouble(operand)}}
}

func asDouble(v *pb.Value) float64 {
	if i, ok := v.GetValueType().(*pb.Value_IntegerValue); ok {
		return float64(i.IntegerValue)
	}
	return v.GetDoubleValue()
}

// document renders an entry, optionally projected through a read mask.
func (db *database) document(e *entry, mask *pb.DocumentMask) (*pb.Document, error) {
	fields := e.fields()
	if mask != nil {
		paths, err := maskPaths(mask.GetFieldPaths())
		if err != nil {
			return nil, err
		}
		fields = project(fields, paths)
	}
	return &pb.Document{Name: db.documentName(e.path), Fields: fields, CreateTime: micros(e.create), UpdateTime: micros(e.update)}, nil
}

func maskPaths(paths []string) ([][]string, error) {
	out := make([][]string, 0, len(paths))
	for _, p := range paths {
		segments, err := parseFieldPath(p)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		out = append(out, segments)
	}
	return out, nil
}

// childCollections lists the collection ids directly below parent ("" for the root) that hold any
// document at any depth, whether or not the parent document itself exists.
func (db *database) childCollections(parent string) []string {
	prefix := ""
	if parent != "" {
		prefix = parent + "/"
	}
	seen := map[string]bool{}
	for path := range db.docs {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		rest := path[len(prefix):]
		if i := strings.IndexByte(rest, '/'); i > 0 {
			seen[rest[:i]] = true
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// missingParents returns ids in a collection that have no document but do have descendants.
func (db *database) missingParents(collection string) []string {
	prefix := collection + "/"
	seen := map[string]bool{}
	for path := range db.docs {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		rest := path[len(prefix):]
		if i := strings.IndexByte(rest, '/'); i > 0 {
			if _, exists := db.docs[prefix+rest[:i]]; !exists {
				seen[rest[:i]] = true
			}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

func errorf(code codes.Code, format string, args ...any) error {
	return status.Error(code, fmt.Sprintf(format, args...))
}
