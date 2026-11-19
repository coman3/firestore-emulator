package main

import (
	"sort"
	"strings"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const namePath = "__name__"

type order struct {
	path       []string // nil means the document name
	descending bool
}

type row struct {
	e      *entry
	fields map[string]*pb.Value
	name   *pb.Value
}

func (r *row) value(path []string) (*pb.Value, bool) {
	if path == nil {
		return r.name, true
	}
	return getField(r.fields, path)
}

func fieldSegments(ref *pb.StructuredQuery_FieldReference) ([]string, error) {
	if ref.GetFieldPath() == namePath {
		return nil, nil
	}
	segments, err := parseFieldPath(ref.GetFieldPath())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return segments, nil
}

// runQuery evaluates a structured query under parent ("" for the root, or a document path).
// The caller holds the database read lock.
func (db *database) runQuery(parent string, q *pb.StructuredQuery) ([]*row, error) {
	if len(q.GetFrom()) != 1 {
		return nil, status.Error(codes.InvalidArgument, "a query must select exactly one collection")
	}
	from := q.GetFrom()[0]
	var candidates []*entry
	if from.GetAllDescendants() {
		prefix := ""
		if parent != "" {
			prefix = parent + "/"
		}
		for collection, docs := range db.collections {
			if !strings.HasPrefix(collection, prefix) {
				continue
			}
			if id := collection[strings.LastIndexByte(collection, '/')+1:]; id != from.GetCollectionId() {
				continue
			}
			for _, e := range docs {
				candidates = append(candidates, e)
			}
		}
	} else {
		collection := from.GetCollectionId()
		if parent != "" {
			collection = parent + "/" + collection
		}
		for _, e := range db.collections[collection] {
			candidates = append(candidates, e)
		}
	}

	orders, err := effectiveOrder(q)
	if err != nil {
		return nil, err
	}
	var rows []*row
	for _, e := range candidates {
		r := &row{e: e, fields: e.fields(), name: &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: db.documentName(e.path)}}}
		matched, err := matches(r, q.GetWhere())
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		// Ordering by a field excludes documents that do not have it.
		present := true
		for _, o := range orders {
			if _, ok := r.value(o.path); !ok {
				present = false
				break
			}
		}
		if present {
			rows = append(rows, r)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return compareRows(rows[i], rows[j], orders) < 0 })

	if c := q.GetStartAt(); c != nil {
		rows = trimStart(rows, orders, c)
	}
	if c := q.GetEndAt(); c != nil {
		rows = trimEnd(rows, orders, c)
	}
	if offset := int(q.GetOffset()); offset > 0 {
		if offset > len(rows) {
			offset = len(rows)
		}
		rows = rows[offset:]
	}
	if limit := q.GetLimit(); limit != nil && int(limit.GetValue()) < len(rows) {
		rows = rows[:limit.GetValue()]
	}
	if q.GetSelect() != nil {
		var paths [][]string
		for _, f := range q.GetSelect().GetFields() {
			segments, err := fieldSegments(f)
			if err != nil {
				return nil, err
			}
			if segments != nil {
				paths = append(paths, segments)
			}
		}
		for _, r := range rows {
			r.fields = project(r.fields, paths)
		}
	}
	return rows, nil
}

// effectiveOrder adds Firestore's implicit ordering: inequality fields not already ordered
// (in field path order), then the document name in the direction of the last explicit order.
func effectiveOrder(q *pb.StructuredQuery) ([]order, error) {
	var orders []order
	seen := map[string]bool{}
	for _, o := range q.GetOrderBy() {
		segments, err := fieldSegments(o.GetField())
		if err != nil {
			return nil, err
		}
		orders = append(orders, order{path: segments, descending: o.GetDirection() == pb.StructuredQuery_DESCENDING})
		seen[o.GetField().GetFieldPath()] = true
	}
	var inequalities []string
	collectInequalities(q.GetWhere(), &inequalities)
	sort.Strings(inequalities)
	descending := len(orders) > 0 && orders[len(orders)-1].descending
	for _, field := range inequalities {
		if seen[field] {
			continue
		}
		seen[field] = true
		ref := &pb.StructuredQuery_FieldReference{FieldPath: field}
		segments, err := fieldSegments(ref)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order{path: segments, descending: descending})
	}
	if !seen[namePath] {
		orders = append(orders, order{path: nil, descending: descending})
	}
	return orders, nil
}

func collectInequalities(f *pb.StructuredQuery_Filter, out *[]string) {
	switch filter := f.GetFilterType().(type) {
	case *pb.StructuredQuery_Filter_CompositeFilter:
		for _, child := range filter.CompositeFilter.GetFilters() {
			collectInequalities(child, out)
		}
	case *pb.StructuredQuery_Filter_FieldFilter:
		switch filter.FieldFilter.GetOp() {
		case pb.StructuredQuery_FieldFilter_LESS_THAN, pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
			pb.StructuredQuery_FieldFilter_GREATER_THAN, pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
			pb.StructuredQuery_FieldFilter_NOT_EQUAL, pb.StructuredQuery_FieldFilter_NOT_IN:
			field := filter.FieldFilter.GetField().GetFieldPath()
			for _, existing := range *out {
				if existing == field {
					return
				}
			}
			*out = append(*out, field)
		}
	case *pb.StructuredQuery_Filter_UnaryFilter:
		switch filter.UnaryFilter.GetOp() {
		case pb.StructuredQuery_UnaryFilter_IS_NOT_NAN, pb.StructuredQuery_UnaryFilter_IS_NOT_NULL:
			field := filter.UnaryFilter.GetField().GetFieldPath()
			for _, existing := range *out {
				if existing == field {
					return
				}
			}
			*out = append(*out, field)
		}
	}
}

func compareRows(a, b *row, orders []order) int {
	for _, o := range orders {
		x, _ := a.value(o.path)
		y, _ := b.value(o.path)
		c := compareValues(x, y)
		if o.descending {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

// compareToCursor compares a row with a cursor position over the cursor's leading orders.
func compareToCursor(r *row, orders []order, cursor []*pb.Value) int {
	for i, v := range cursor {
		if i >= len(orders) {
			break
		}
		x, _ := r.value(orders[i].path)
		c := compareValues(x, cursorValue(orders[i], v, r))
		if orders[i].descending {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

// A document-name cursor may be given relative to the queried collection.
func cursorValue(o order, v *pb.Value, r *row) *pb.Value {
	if o.path != nil {
		return v
	}
	ref, ok := v.GetValueType().(*pb.Value_ReferenceValue)
	if !ok || strings.HasPrefix(ref.ReferenceValue, "projects/") {
		return v
	}
	name := r.name.GetReferenceValue()
	parent := name[:strings.LastIndexByte(name, '/')]
	return &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: parent + "/" + ref.ReferenceValue}}
}

func trimStart(rows []*row, orders []order, c *pb.Cursor) []*row {
	for i, r := range rows {
		cmp := compareToCursor(r, orders, c.GetValues())
		if cmp > 0 || (cmp == 0 && c.GetBefore()) {
			return rows[i:]
		}
	}
	return nil
}

func trimEnd(rows []*row, orders []order, c *pb.Cursor) []*row {
	for i, r := range rows {
		cmp := compareToCursor(r, orders, c.GetValues())
		if cmp > 0 || (cmp == 0 && c.GetBefore()) {
			return rows[:i]
		}
	}
	return rows
}

func matches(r *row, f *pb.StructuredQuery_Filter) (bool, error) {
	switch filter := f.GetFilterType().(type) {
	case nil:
		return true, nil
	case *pb.StructuredQuery_Filter_CompositeFilter:
		or := filter.CompositeFilter.GetOp() == pb.StructuredQuery_CompositeFilter_OR
		if len(filter.CompositeFilter.GetFilters()) == 0 {
			return true, nil
		}
		for _, child := range filter.CompositeFilter.GetFilters() {
			ok, err := matches(r, child)
			if err != nil {
				return false, err
			}
			if or && ok {
				return true, nil
			}
			if !or && !ok {
				return false, nil
			}
		}
		return !or, nil
	case *pb.StructuredQuery_Filter_UnaryFilter:
		segments, err := fieldSegments(filter.UnaryFilter.GetField())
		if err != nil {
			return false, err
		}
		v, ok := r.value(segments)
		if !ok {
			return false, nil
		}
		switch filter.UnaryFilter.GetOp() {
		case pb.StructuredQuery_UnaryFilter_IS_NAN:
			return isNaN(v), nil
		case pb.StructuredQuery_UnaryFilter_IS_NULL:
			return isNull(v), nil
		case pb.StructuredQuery_UnaryFilter_IS_NOT_NAN:
			return !isNaN(v), nil
		case pb.StructuredQuery_UnaryFilter_IS_NOT_NULL:
			return !isNull(v), nil
		}
		return false, status.Error(codes.InvalidArgument, "unsupported unary filter")
	case *pb.StructuredQuery_Filter_FieldFilter:
		segments, err := fieldSegments(filter.FieldFilter.GetField())
		if err != nil {
			return false, err
		}
		v, ok := r.value(segments)
		if !ok {
			return false, nil
		}
		target := filter.FieldFilter.GetValue()
		if segments == nil {
			target = resolveNameFilterValue(target, r)
		}
		return fieldMatches(filter.FieldFilter.GetOp(), v, target)
	}
	return false, status.Error(codes.InvalidArgument, "unsupported filter")
}

func resolveNameFilterValue(target *pb.Value, r *row) *pb.Value {
	if a, ok := target.GetValueType().(*pb.Value_ArrayValue); ok {
		values := make([]*pb.Value, 0, len(a.ArrayValue.GetValues()))
		for _, v := range a.ArrayValue.GetValues() {
			values = append(values, cursorValue(order{}, v, r))
		}
		return arrayValue(values)
	}
	return cursorValue(order{}, target, r)
}

func fieldMatches(op pb.StructuredQuery_FieldFilter_Operator, v, target *pb.Value) (bool, error) {
	switch op {
	case pb.StructuredQuery_FieldFilter_EQUAL:
		return valuesEqual(v, target), nil
	case pb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return !isNull(v) && !valuesEqual(v, target), nil
	case pb.StructuredQuery_FieldFilter_LESS_THAN, pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
		pb.StructuredQuery_FieldFilter_GREATER_THAN, pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		// Range filters only match values of the same type; NaN matches no range.
		if typeOrder(v) != typeOrder(target) || isNaN(v) || isNaN(target) {
			return false, nil
		}
		c := compareValues(v, target)
		switch op {
		case pb.StructuredQuery_FieldFilter_LESS_THAN:
			return c < 0, nil
		case pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
			return c <= 0, nil
		case pb.StructuredQuery_FieldFilter_GREATER_THAN:
			return c > 0, nil
		default:
			return c >= 0, nil
		}
	case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
		return containsValue(v.GetArrayValue().GetValues(), target), nil
	case pb.StructuredQuery_FieldFilter_IN:
		// A null in the list never matches: nulls are only found with IS_NULL.
		return !isNull(v) && containsValue(target.GetArrayValue().GetValues(), v), nil
	case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		for _, candidate := range target.GetArrayValue().GetValues() {
			if containsValue(v.GetArrayValue().GetValues(), candidate) {
				return true, nil
			}
		}
		return false, nil
	case pb.StructuredQuery_FieldFilter_NOT_IN:
		return !isNull(v) && !containsValue(target.GetArrayValue().GetValues(), v), nil
	}
	return false, status.Errorf(codes.InvalidArgument, "unsupported field filter %v", op)
}

// aggregate computes count, sum and average over query rows.
func aggregate(rows []*row, aggregations []*pb.StructuredAggregationQuery_Aggregation) (map[string]*pb.Value, error) {
	out := map[string]*pb.Value{}
	for i, a := range aggregations {
		alias := a.GetAlias()
		if alias == "" {
			alias = "field_" + string(rune('1'+i))
		}
		switch op := a.GetOperator().(type) {
		case *pb.StructuredAggregationQuery_Aggregation_Count_:
			n := int64(len(rows))
			if up := op.Count.GetUpTo(); up != nil && up.GetValue() < n {
				n = up.GetValue()
			}
			out[alias] = &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: n}}
		case *pb.StructuredAggregationQuery_Aggregation_Sum_, *pb.StructuredAggregationQuery_Aggregation_Avg_:
			var field *pb.StructuredQuery_FieldReference
			if s, ok := op.(*pb.StructuredAggregationQuery_Aggregation_Sum_); ok {
				field = s.Sum.GetField()
			} else {
				field = op.(*pb.StructuredAggregationQuery_Aggregation_Avg_).Avg.GetField()
			}
			segments, err := fieldSegments(field)
			if err != nil {
				return nil, err
			}
			sum := &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: 0}}
			count := 0
			for _, r := range rows {
				if v, ok := r.value(segments); ok && isNumber(v) {
					sum = addExact(sum, v)
					count++
				}
			}
			if _, isSum := op.(*pb.StructuredAggregationQuery_Aggregation_Sum_); isSum {
				out[alias] = sum
			} else if count == 0 {
				out[alias] = nullValue()
			} else {
				out[alias] = &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: asDouble(sum) / float64(count)}}
			}
		default:
			return nil, status.Error(codes.InvalidArgument, "unsupported aggregation")
		}
	}
	return out, nil
}

// addExact sums integers as integers until one overflows or a double appears.
func addExact(a, b *pb.Value) *pb.Value {
	x, xInt := a.GetValueType().(*pb.Value_IntegerValue)
	y, yInt := b.GetValueType().(*pb.Value_IntegerValue)
	if xInt && yInt {
		sum := x.IntegerValue + y.IntegerValue
		if (y.IntegerValue > 0 && sum >= x.IntegerValue) || (y.IntegerValue <= 0 && sum <= x.IntegerValue) {
			return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: sum}}
		}
	}
	return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: asDouble(a) + asDouble(b)}}
}
