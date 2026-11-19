package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	gocmp "github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// testdata/java-export was written by the Java emulator (v1.22.0) and documents.json is what that
// emulator served over REST for the same data.

func dbName(project string) string { return "projects/" + project + "/databases/(default)" }

func str(s string) *pb.Value  { return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: s}} }
func num(i int64) *pb.Value   { return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: i}} }
func dbl(f float64) *pb.Value { return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: f}} }

func expectedDocuments(t *testing.T) map[string]*pb.MapValue {
	t.Helper()
	data, err := os.ReadFile("testdata/java-export/documents.json")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	out := map[string]*pb.MapValue{}
	for name, fields := range raw {
		m := &pb.MapValue{}
		if err := protojson.Unmarshal([]byte(`{"fields":`+string(fields)+`}`), m); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		out[name] = m
	}
	return out
}

func storedDocuments(s *Store) map[string]*pb.MapValue {
	out := map[string]*pb.MapValue{}
	for _, db := range s.dbs {
		for path, e := range db.docs {
			out[db.documentName(path)] = &pb.MapValue{Fields: e.fields()}
		}
	}
	return out
}

func importFixture(t *testing.T, s *Store, root string) {
	t.Helper()
	for _, project := range []string{"p1", "p2"} {
		if _, err := s.importDatabase(dbName(project), filepath.Join(root, project, project+".overall_export_metadata")); err != nil {
			t.Fatal(err)
		}
	}
}

func TestImportsJavaEmulatorExport(t *testing.T) {
	s := NewStore()
	importFixture(t, s, "testdata/java-export")
	if diff := gocmp.Diff(expectedDocuments(t), storedDocuments(s), protocmp.Transform(), cmpopts.EquateNaNs()); diff != "" {
		t.Fatalf("imported documents differ from what the Java emulator served (-want +got):\n%s", diff)
	}
}

func TestImportsEmptyJavaNamespace(t *testing.T) {
	// Native Java 1.22.0 :export of a database with no documents.
	metadata, err := hex.DecodeString("b86d444e01000133052b2843000001")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "empty.overall_export_metadata")
	if err := os.WriteFile(file, metadata, 0o600); err != nil {
		t.Fatal(err)
	}
	s := NewStore()
	count, err := s.importDatabase(dbName("empty"), file)
	if err != nil || count != 0 {
		t.Fatalf("empty Java import: count=%d err=%v", count, err)
	}
	if len(storedDocuments(s)) != 0 {
		t.Fatal("empty import created documents")
	}
	for _, invalid := range [][]byte{metadata[:8], metadata[:len(metadata)-1], {}} {
		if err := os.WriteFile(file, invalid, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := s.importDatabase(dbName("empty"), file); err == nil {
			t.Fatal("incomplete empty export accepted")
		}
	}
}

func TestExportRoundTripsAndMatchesJavaLayout(t *testing.T) {
	s := NewStore()
	importFixture(t, s, "testdata/java-export")
	dir := t.TempDir()
	for _, project := range []string{"p1", "p2"} {
		if err := s.exportDatabase(dbName(project), dir, project); err != nil {
			t.Fatal(err)
		}
	}
	// A single-document export, and every overall metadata file, are byte-identical to Java's.
	for _, file := range []string{"p2/all_namespaces/all_kinds/output-0", "p1/p1.overall_export_metadata", "p2/p2.overall_export_metadata"} {
		want, _ := os.ReadFile(filepath.Join("testdata/java-export", file))
		got, _ := os.ReadFile(filepath.Join(dir, file))
		if !bytes.Equal(want, got) {
			t.Errorf("%s differs from the Java emulator's export", file)
		}
	}
	again := NewStore()
	importFixture(t, again, dir)
	if diff := gocmp.Diff(storedDocuments(s), storedDocuments(again), protocmp.Transform(), cmpopts.EquateNaNs()); diff != "" {
		t.Fatalf("export round trip changed documents (-want +got):\n%s", diff)
	}
}

func TestExportSplitsLargeDocumentsAcrossLogBlocks(t *testing.T) {
	s := NewStore()
	big := strings.Repeat("0123456789", 10_000) // 100 KB spans several 32 KiB log blocks
	for i, path := range []string{"big/a", "big/b", "big/c"} {
		w := &pb.Write{Operation: &pb.Write_Update{Update: &pb.Document{Name: dbName("p") + "/documents/" + path,
			Fields: map[string]*pb.Value{"body": str(big), "i": num(int64(i))}}}}
		if _, err := s.commit(dbName("p"), []*pb.Write{w}, nil); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	if err := s.exportDatabase(dbName("p"), dir, "x"); err != nil {
		t.Fatal(err)
	}
	again := NewStore()
	if n, err := again.importDatabase(dbName("p"), filepath.Join(dir, "x")); err != nil || n != 3 {
		t.Fatalf("imported %d documents, err %v", n, err)
	}
	if diff := gocmp.Diff(storedDocuments(s), storedDocuments(again), protocmp.Transform(), cmpopts.EquateNaNs()); diff != "" {
		t.Fatal(diff)
	}
}

func TestValueOrdering(t *testing.T) {
	ordered := []*pb.Value{
		nullValue(),
		{ValueType: &pb.Value_BooleanValue{BooleanValue: false}},
		dbl(math.NaN()), dbl(math.Inf(-1)), num(-3), dbl(1), num(2), dbl(2.5), num(math.MaxInt64),
		str(""), str("a"), str("é"),
		{ValueType: &pb.Value_BytesValue{BytesValue: []byte{0}}},
		{ValueType: &pb.Value_ReferenceValue{ReferenceValue: "projects/p/databases/(default)/documents/a/b"}},
		{ValueType: &pb.Value_ReferenceValue{ReferenceValue: "projects/p/databases/(default)/documents/a/b/c/d"}},
		arrayValue([]*pb.Value{num(1)}),
		arrayValue([]*pb.Value{num(1), num(0)}),
		{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: map[string]*pb.Value{"a": num(1)}}}},
	}
	for i := 0; i+1 < len(ordered); i++ {
		if c := compareValues(ordered[i], ordered[i+1]); c >= 0 {
			t.Errorf("expected %v < %v, got %d", ordered[i], ordered[i+1], c)
		}
	}
	if compareIntDouble(math.MaxInt64, 9.223372036854775807e18) != -1 {
		t.Error("int64 max must sort below the double that rounds above it")
	}
	if !valuesEqual(num(1), dbl(1)) || valuesEqual(dbl(math.NaN()), dbl(math.NaN())) {
		t.Error("query equality must match 1 with 1.0 and never match NaN")
	}
}

func TestParseFieldPath(t *testing.T) {
	cases := map[string][]string{"a": {"a"}, "a.b": {"a", "b"}, "`a.b`.c": {"a.b", "c"}, "`back\\`tick`": {"back`tick"}}
	for in, want := range cases {
		got, err := parseFieldPath(in)
		if err != nil || !gocmp.Equal(got, want) {
			t.Errorf("parseFieldPath(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "a.", ".a", "`open"} {
		if _, err := parseFieldPath(bad); err == nil {
			t.Errorf("parseFieldPath(%q) should fail", bad)
		}
	}
}

func set(name string, fields map[string]*pb.Value, pre *pb.Precondition) *pb.Write {
	return &pb.Write{Operation: &pb.Write_Update{Update: &pb.Document{Name: dbName("p") + "/documents/" + name, Fields: fields}}, CurrentDocument: pre}
}

func TestCommitPreconditionsAndAtomicity(t *testing.T) {
	s := NewStore()
	mustNotExist := &pb.Precondition{ConditionType: &pb.Precondition_Exists{Exists: false}}
	if _, err := s.commit(dbName("p"), []*pb.Write{set("c/a", map[string]*pb.Value{"n": num(1)}, mustNotExist)}, nil); err != nil {
		t.Fatal(err)
	}
	_, err := s.commit(dbName("p"), []*pb.Write{
		set("c/b", map[string]*pb.Value{"n": num(2)}, nil),
		set("c/a", map[string]*pb.Value{"n": num(3)}, mustNotExist),
	}, nil)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected ALREADY_EXISTS, got %v", err)
	}
	if s.database(dbName("p")).docs["c/b"] != nil {
		t.Fatal("a failed commit must not apply any of its writes")
	}
}

func TestNoOpWriteKeepsUpdateTime(t *testing.T) {
	s := NewStore()
	w := set("c/a", map[string]*pb.Value{"n": num(1)}, nil)
	first, _ := s.commit(dbName("p"), []*pb.Write{w}, nil)
	second, _ := s.commit(dbName("p"), []*pb.Write{proto.Clone(w).(*pb.Write)}, nil)
	if !proto.Equal(first.WriteResults[0].UpdateTime, second.WriteResults[0].UpdateTime) {
		t.Fatal("rewriting identical content must keep the previous update time")
	}
}

func TestRESTRefusesRequestsABrowserPageCouldSend(t *testing.T) {
	handler := &restHandler{server: &Server{store: NewStore()}}
	request := func(host, origin, contentType string) int {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
		r.Host = host
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if contentType != "" {
			r.Header.Set("Content-Type", contentType)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	for _, c := range []struct {
		name, host, origin, contentType string
		refused                         bool
	}{
		{"local tool", "127.0.0.1:8080", "", "application/json", false},
		{"local page", "localhost:8080", "https://localhost:4300", "application/json; charset=utf-8", false},
		{"another site", "localhost:8080", "https://example.com", "application/json", true},
		{"DNS rebinding", "attacker.example:8080", "", "application/json", true},
		{"form-style post", "localhost:8080", "", "text/plain", true},
	} {
		if got := request(c.host, c.origin, c.contentType); (got == http.StatusForbidden || got == http.StatusBadRequest) != c.refused {
			t.Errorf("%s: got HTTP %d", c.name, got)
		}
	}
}

func TestTransactionAbortsWhenAQueryItRanGainsADocument(t *testing.T) {
	s := NewStore()
	server := &Server{store: s}
	query := func(tx []byte) *pb.RunQueryRequest {
		return &pb.RunQueryRequest{Parent: dbName("p") + "/documents",
			QueryType:           &pb.RunQueryRequest_StructuredQuery{StructuredQuery: &pb.StructuredQuery{From: []*pb.StructuredQuery_CollectionSelector{{CollectionId: "c"}}}},
			ConsistencySelector: &pb.RunQueryRequest_Transaction{Transaction: tx}}
	}

	// Two transactions both find the collection empty and add to it: the second to commit must retry.
	first, second := s.beginTransaction(dbName("p"), nil), s.beginTransaction(dbName("p"), nil)
	for _, tx := range [][]byte{first, second} {
		if _, err := server.runQuery(query(tx)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.commit(dbName("p"), []*pb.Write{set("c/a", map[string]*pb.Value{"n": num(1)}, nil)}, first); err != nil {
		t.Fatal(err)
	}
	if _, err := s.commit(dbName("p"), []*pb.Write{set("c/b", map[string]*pb.Value{"n": num(2)}, nil)}, second); status.Code(err) != codes.Aborted {
		t.Fatalf("expected ABORTED, got %v", err)
	}

	// An unchanged result set commits.
	third := s.beginTransaction(dbName("p"), nil)
	if _, err := server.runQuery(query(third)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.commit(dbName("p"), []*pb.Write{set("c/b", map[string]*pb.Value{"n": num(2)}, nil)}, third); err != nil {
		t.Fatalf("expected the commit to succeed, got %v", err)
	}
}

func TestTransactionAbortsAfterConcurrentChange(t *testing.T) {
	s := NewStore()
	server := &Server{store: s}
	s.commit(dbName("p"), []*pb.Write{set("c/a", map[string]*pb.Value{"n": num(1)}, nil)}, nil)
	tx := s.beginTransaction(dbName("p"), nil)
	if _, err := server.batchGet(&pb.BatchGetDocumentsRequest{Database: dbName("p"), Documents: []string{dbName("p") + "/documents/c/a"},
		ConsistencySelector: &pb.BatchGetDocumentsRequest_Transaction{Transaction: tx}}); err != nil {
		t.Fatal(err)
	}
	s.commit(dbName("p"), []*pb.Write{set("c/a", map[string]*pb.Value{"n": num(2)}, nil)}, nil)
	_, err := s.commit(dbName("p"), []*pb.Write{set("c/a", map[string]*pb.Value{"n": num(10)}, nil)}, tx)
	if status.Code(err) != codes.Aborted {
		t.Fatalf("expected ABORTED, got %v", err)
	}
}
