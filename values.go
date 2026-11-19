package main

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Firestore orders values first by type, then within the type.
const (
	orderNull = iota
	orderBool
	orderNumber
	orderTimestamp
	orderString
	orderBytes
	orderReference
	orderGeoPoint
	orderArray
	orderMap
)

func typeOrder(v *pb.Value) int {
	switch v.GetValueType().(type) {
	case *pb.Value_BooleanValue:
		return orderBool
	case *pb.Value_IntegerValue, *pb.Value_DoubleValue:
		return orderNumber
	case *pb.Value_TimestampValue:
		return orderTimestamp
	case *pb.Value_StringValue:
		return orderString
	case *pb.Value_BytesValue:
		return orderBytes
	case *pb.Value_ReferenceValue:
		return orderReference
	case *pb.Value_GeoPointValue:
		return orderGeoPoint
	case *pb.Value_ArrayValue:
		return orderArray
	case *pb.Value_MapValue:
		return orderMap
	default:
		return orderNull
	}
}

func cmp[T int | int64 | float64 | string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// compareValues implements Firestore's total order over values.
func compareValues(a, b *pb.Value) int {
	ta, tb := typeOrder(a), typeOrder(b)
	if ta != tb {
		return cmp(ta, tb)
	}
	switch ta {
	case orderBool:
		return cmp(boolInt(a.GetBooleanValue()), boolInt(b.GetBooleanValue()))
	case orderNumber:
		return compareNumbers(a, b)
	case orderTimestamp:
		x, y := a.GetTimestampValue(), b.GetTimestampValue()
		if c := cmp(x.GetSeconds(), y.GetSeconds()); c != 0 {
			return c
		}
		return cmp(int64(x.GetNanos()), int64(y.GetNanos()))
	case orderString:
		return strings.Compare(a.GetStringValue(), b.GetStringValue())
	case orderBytes:
		return bytes.Compare(a.GetBytesValue(), b.GetBytesValue())
	case orderReference:
		return compareReferences(a.GetReferenceValue(), b.GetReferenceValue())
	case orderGeoPoint:
		x, y := a.GetGeoPointValue(), b.GetGeoPointValue()
		if c := compareDoubles(x.GetLatitude(), y.GetLatitude()); c != 0 {
			return c
		}
		return compareDoubles(x.GetLongitude(), y.GetLongitude())
	case orderArray:
		x, y := a.GetArrayValue().GetValues(), b.GetArrayValue().GetValues()
		for i := 0; i < len(x) && i < len(y); i++ {
			if c := compareValues(x[i], y[i]); c != 0 {
				return c
			}
		}
		return cmp(len(x), len(y))
	case orderMap:
		return compareMaps(a.GetMapValue().GetFields(), b.GetMapValue().GetFields())
	}
	return 0
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func compareMaps(x, y map[string]*pb.Value) int {
	kx, ky := sortedKeys(x), sortedKeys(y)
	for i := 0; i < len(kx) && i < len(ky); i++ {
		if c := strings.Compare(kx[i], ky[i]); c != 0 {
			return c
		}
		if c := compareValues(x[kx[i]], y[ky[i]]); c != 0 {
			return c
		}
	}
	return cmp(len(kx), len(ky))
}

func sortedKeys(m map[string]*pb.Value) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// NaN sorts before every other number and equals itself for ordering purposes.
func compareDoubles(a, b float64) int {
	an, bn := math.IsNaN(a), math.IsNaN(b)
	switch {
	case an && bn:
		return 0
	case an:
		return -1
	case bn:
		return 1
	}
	return cmp(a, b)
}

func compareNumbers(a, b *pb.Value) int {
	ai, aInt := a.GetValueType().(*pb.Value_IntegerValue)
	bi, bInt := b.GetValueType().(*pb.Value_IntegerValue)
	switch {
	case aInt && bInt:
		return cmp(ai.IntegerValue, bi.IntegerValue)
	case aInt:
		return compareIntDouble(ai.IntegerValue, b.GetDoubleValue())
	case bInt:
		return -compareIntDouble(bi.IntegerValue, a.GetDoubleValue())
	}
	return compareDoubles(a.GetDoubleValue(), b.GetDoubleValue())
}

// compareIntDouble compares exactly, without rounding the integer through float64.
func compareIntDouble(i int64, d float64) int {
	switch {
	case math.IsNaN(d):
		return 1
	case d < -9.223372036854775808e18:
		return 1
	case d >= 9.223372036854775808e18:
		return -1
	}
	t := math.Trunc(d)
	if c := cmp(i, int64(t)); c != 0 {
		return c
	}
	return cmp(t, d)
}

func compareReferences(a, b string) int {
	x, y := strings.Split(a, "/"), strings.Split(b, "/")
	for i := 0; i < len(x) && i < len(y); i++ {
		if c := strings.Compare(x[i], y[i]); c != 0 {
			return c
		}
	}
	return cmp(len(x), len(y))
}

// valuesEqual is query equality: numbers compare across integer and double, NaN equals nothing.
func valuesEqual(a, b *pb.Value) bool {
	ta, tb := typeOrder(a), typeOrder(b)
	if ta != tb {
		return false
	}
	switch ta {
	case orderNumber:
		if isNaN(a) || isNaN(b) {
			return false
		}
		return compareNumbers(a, b) == 0
	case orderArray:
		x, y := a.GetArrayValue().GetValues(), b.GetArrayValue().GetValues()
		if len(x) != len(y) {
			return false
		}
		for i := range x {
			if !valuesEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	case orderMap:
		x, y := a.GetMapValue().GetFields(), b.GetMapValue().GetFields()
		if len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !valuesEqual(v, w) {
				return false
			}
		}
		return true
	}
	return compareValues(a, b) == 0
}

func isNaN(v *pb.Value) bool {
	d, ok := v.GetValueType().(*pb.Value_DoubleValue)
	return ok && math.IsNaN(d.DoubleValue)
}

func isNull(v *pb.Value) bool { return typeOrder(v) == orderNull }

func isNumber(v *pb.Value) bool { return typeOrder(v) == orderNumber }

func nullValue() *pb.Value { return &pb.Value{ValueType: &pb.Value_NullValue{}} }

// Firestore stores timestamps with microsecond precision.
func truncateTimestamps(v *pb.Value) {
	switch t := v.GetValueType().(type) {
	case *pb.Value_TimestampValue:
		t.TimestampValue = timestamppb.New(t.TimestampValue.AsTime().Truncate(1000))
	case *pb.Value_ArrayValue:
		for _, e := range t.ArrayValue.GetValues() {
			truncateTimestamps(e)
		}
	case *pb.Value_MapValue:
		for _, e := range t.MapValue.GetFields() {
			truncateTimestamps(e)
		}
	}
}

// parseFieldPath splits a Firestore field path, honouring backtick-quoted segments.
func parseFieldPath(path string) ([]string, error) {
	if path == "" {
		return nil, fmt.Errorf("empty field path")
	}
	var segments []string
	var current strings.Builder
	quoted, escaped, started := false, false, false
	for _, r := range path {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case quoted && r == '\\':
			escaped = true
		case r == '`':
			quoted = !quoted
			started = true
		case r == '.' && !quoted:
			if !started && current.Len() == 0 {
				return nil, fmt.Errorf("invalid field path %q", path)
			}
			segments = append(segments, current.String())
			current.Reset()
			started = false
		default:
			current.WriteRune(r)
			started = true
		}
	}
	if quoted || escaped || (!started && current.Len() == 0) {
		return nil, fmt.Errorf("invalid field path %q", path)
	}
	return append(segments, current.String()), nil
}

func getField(fields map[string]*pb.Value, path []string) (*pb.Value, bool) {
	for i, segment := range path {
		v, ok := fields[segment]
		if !ok {
			return nil, false
		}
		if i == len(path)-1 {
			return v, true
		}
		m, isMap := v.GetValueType().(*pb.Value_MapValue)
		if !isMap {
			return nil, false
		}
		fields = m.MapValue.GetFields()
	}
	return nil, false
}

func setField(fields map[string]*pb.Value, path []string, value *pb.Value) {
	for _, segment := range path[:len(path)-1] {
		v, ok := fields[segment]
		m, isMap := v.GetValueType().(*pb.Value_MapValue)
		if !ok || !isMap {
			m = &pb.Value_MapValue{MapValue: &pb.MapValue{}}
			fields[segment] = &pb.Value{ValueType: m}
		}
		if m.MapValue.Fields == nil {
			m.MapValue.Fields = map[string]*pb.Value{}
		}
		fields = m.MapValue.Fields
	}
	fields[path[len(path)-1]] = value
}

func deleteField(fields map[string]*pb.Value, path []string) {
	for _, segment := range path[:len(path)-1] {
		m, isMap := fields[segment].GetValueType().(*pb.Value_MapValue)
		if !isMap {
			return
		}
		fields = m.MapValue.GetFields()
	}
	delete(fields, path[len(path)-1])
}

// project keeps only the masked paths, as a read mask or query projection does.
func project(fields map[string]*pb.Value, paths [][]string) map[string]*pb.Value {
	out := map[string]*pb.Value{}
	for _, path := range paths {
		if v, ok := getField(fields, path); ok {
			setField(out, path, v)
		}
	}
	return out
}
