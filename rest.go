package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// restHandler serves the Firestore REST API (v1) and the emulator import, export and reset endpoints.
type restHandler struct {
	server *Server
}

var (
	jsonIn  = protojson.UnmarshalOptions{DiscardUnknown: true}
	jsonOut = protojson.MarshalOptions{}
)

// fromLocalTool refuses what a web page open in the developer's browser could send. The emulator only listens on
// loopback, but a page can still reach it: from another site (a non-local Origin), through a DNS-rebinding hostname
// (Host), or as a form-style POST that skips the CORS preflight (a body that isn't JSON).
func fromLocalTool(r *http.Request) error {
	if host := hostname(r.Host); host != "localhost" && !strings.HasSuffix(host, ".localhost") && net.ParseIP(host) == nil {
		return status.Errorf(codes.PermissionDenied, "host %q is not local", r.Host)
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if ip := net.ParseIP(hostname(u.Host)); err != nil || (hostname(u.Host) != "localhost" && (ip == nil || !ip.IsLoopback())) {
			return status.Errorf(codes.PermissionDenied, "requests from %s are not allowed", origin)
		}
	}
	if r.ContentLength != 0 && r.Method != http.MethodGet {
		if mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mediaType != "application/json" {
			return status.Error(codes.InvalidArgument, "request bodies must be application/json")
		}
	}
	return nil
}

func hostname(hostport string) string {
	if host, _, err := net.SplitHostPort(hostport); err == nil {
		return strings.ToLower(host)
	}
	return strings.ToLower(strings.Trim(hostport, "[]"))
}

func (h *restHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := fromLocalTool(r); err != nil {
		writeError(w, err)
		return
	}
	p := r.URL.Path
	switch {
	case p == "/":
		io.WriteString(w, "Ok\n")
	case strings.HasPrefix(p, "/emulator/v1/projects/"):
		h.emulator(w, r, strings.TrimPrefix(p, "/emulator/v1/"))
	case strings.HasPrefix(p, "/v1/projects/"):
		h.v1(w, r, strings.TrimPrefix(p, "/v1/"))
	default:
		writeError(w, status.Errorf(codes.NotFound, "no route for %s", p))
	}
}

func splitMethod(resource string) (string, string) {
	if i := strings.LastIndexByte(resource, ':'); i > strings.LastIndexByte(resource, '/') {
		return resource[:i], resource[i+1:]
	}
	return resource, ""
}

func (h *restHandler) v1(w http.ResponseWriter, r *http.Request, resource string) {
	resource, method := splitMethod(resource)
	database := strings.TrimSuffix(resource, "/documents")
	s := h.server
	switch {
	case method != "" && r.Method != http.MethodPost:
		writeError(w, status.Errorf(codes.Unimplemented, "%s requires POST", method))
	case method == "commit":
		req := &pb.CommitRequest{}
		if readBody(w, r, req) {
			req.Database = database
			respond(w, result(s.store.commit(req.GetDatabase(), req.GetWrites(), req.GetTransaction())))
		}
	case method == "batchWrite":
		req := &pb.BatchWriteRequest{}
		if readBody(w, r, req) {
			req.Database = database
			respond(w, result(s.BatchWrite(r.Context(), req)))
		}
	case method == "batchGet":
		req := &pb.BatchGetDocumentsRequest{}
		if readBody(w, r, req) {
			req.Database = database
			responses, err := s.batchGet(req)
			respondList(w, responses, err)
		}
	case method == "beginTransaction":
		req := &pb.BeginTransactionRequest{}
		if readBody(w, r, req) {
			req.Database = database
			respond(w, result(s.BeginTransaction(r.Context(), req)))
		}
	case method == "rollback":
		req := &pb.RollbackRequest{}
		if readBody(w, r, req) {
			req.Database = database
			_, err := s.Rollback(r.Context(), req)
			respond(w, outcome{&pb.Document{}, err})
		}
	case method == "runQuery":
		req := &pb.RunQueryRequest{}
		if readBody(w, r, req) {
			req.Parent = resource
			responses, err := s.runQuery(req)
			respondList(w, responses, err)
		}
	case method == "runAggregationQuery":
		req := &pb.RunAggregationQueryRequest{}
		if readBody(w, r, req) {
			req.Parent = resource
			response, err := s.runAggregationQuery(req)
			respondList(w, []*pb.RunAggregationQueryResponse{response}, err)
		}
	case method == "listCollectionIds":
		req := &pb.ListCollectionIdsRequest{}
		if readBody(w, r, req) {
			req.Parent = resource
			respond(w, result(s.ListCollectionIds(r.Context(), req)))
		}
	case method != "":
		writeError(w, status.Errorf(codes.Unimplemented, "method %s is not supported", method))
	default:
		h.documents(w, r, resource)
	}
}

// documents handles REST calls addressed to a document or collection path.
func (h *restHandler) documents(w http.ResponseWriter, r *http.Request, resource string) {
	_, path, err := parseName(resource)
	if err != nil {
		writeError(w, err)
		return
	}
	query := r.URL.Query()
	s := h.server
	collection := path != "" && strings.Count(path, "/")%2 == 0
	switch {
	case r.Method == http.MethodGet && collection:
		parent, id := splitPath(resource)
		req := &pb.ListDocumentsRequest{Parent: parent, CollectionId: id, PageToken: query.Get("pageToken"),
			ShowMissing: query.Get("showMissing") == "true", Mask: queryMask(query, "mask.fieldPaths")}
		if size, err := strconv.Atoi(query.Get("pageSize")); err == nil {
			req.PageSize = int32(size)
		}
		respond(w, result(s.ListDocuments(r.Context(), req)))
	case r.Method == http.MethodGet:
		respond(w, result(s.GetDocument(r.Context(), &pb.GetDocumentRequest{Name: resource, Mask: queryMask(query, "mask.fieldPaths")})))
	case r.Method == http.MethodPost && collection:
		doc := &pb.Document{}
		if readBody(w, r, doc) {
			parent, id := splitPath(resource)
			respond(w, result(s.CreateDocument(r.Context(), &pb.CreateDocumentRequest{Parent: parent, CollectionId: id,
				DocumentId: query.Get("documentId"), Document: doc, Mask: queryMask(query, "mask.fieldPaths")})))
		}
	case r.Method == http.MethodPatch:
		doc := &pb.Document{}
		if readBody(w, r, doc) {
			doc.Name = resource
			respond(w, result(s.UpdateDocument(r.Context(), &pb.UpdateDocumentRequest{Document: doc,
				UpdateMask: queryMask(query, "updateMask.fieldPaths"), Mask: queryMask(query, "mask.fieldPaths"),
				CurrentDocument: queryPrecondition(query)})))
		}
	case r.Method == http.MethodDelete:
		_, err := s.DeleteDocument(r.Context(), &pb.DeleteDocumentRequest{Name: resource, CurrentDocument: queryPrecondition(query)})
		respond(w, outcome{&pb.Document{}, err})
	default:
		writeError(w, status.Errorf(codes.Unimplemented, "%s %s is not supported", r.Method, resource))
	}
}

func queryMask(query url.Values, key string) *pb.DocumentMask {
	if paths, ok := query[key]; ok {
		return &pb.DocumentMask{FieldPaths: paths}
	}
	return nil
}

func queryPrecondition(query url.Values) *pb.Precondition {
	if v := query.Get("currentDocument.exists"); v != "" {
		return &pb.Precondition{ConditionType: &pb.Precondition_Exists{Exists: v == "true"}}
	}
	if v := query.Get("currentDocument.updateTime"); v != "" {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return &pb.Precondition{ConditionType: &pb.Precondition_UpdateTime{UpdateTime: timestamppb.New(t)}}
		}
	}
	return nil
}

func (h *restHandler) emulator(w http.ResponseWriter, r *http.Request, resource string) {
	resource, method := splitMethod(resource)
	store := h.server.store
	switch {
	case r.Method == http.MethodDelete && method == "":
		db, _, err := parseName(resource)
		if err != nil {
			writeError(w, err)
			return
		}
		d := store.database(db)
		d.mu.Lock()
		d.clear()
		d.mu.Unlock()
		io.WriteString(w, "{}\n")
	case r.Method == http.MethodPost && (method == "export" || method == "import"):
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil && err != io.EOF {
			writeError(w, status.Error(codes.InvalidArgument, err.Error()))
			return
		}
		pick := func(keys ...string) string {
			for _, k := range keys {
				if body[k] != "" {
					return body[k]
				}
			}
			return ""
		}
		db := pick("database")
		if db == "" {
			db = resource + "/databases/(default)"
		}
		directory := pick("export_directory", "exportDirectory")
		if method == "export" {
			err := store.exportDatabase(db, directory, pick("export_name", "exportName"))
			respond(w, outcome{&pb.Document{}, err})
			return
		}
		if _, err := store.importDatabase(db, directory); err != nil {
			writeError(w, toStatus(err))
			return
		}
		io.WriteString(w, "{}\n")
	default:
		writeError(w, status.Errorf(codes.NotFound, "no emulator route for %s %s", r.Method, resource))
	}
}

func toStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.FailedPrecondition, err.Error())
}

func readBody(w http.ResponseWriter, r *http.Request, message proto.Message) bool {
	data, err := io.ReadAll(r.Body)
	if err == nil && len(bytes.TrimSpace(data)) > 0 {
		err = jsonIn.Unmarshal(data, message)
	}
	if err != nil {
		writeError(w, status.Error(codes.InvalidArgument, err.Error()))
		return false
	}
	return true
}

type outcome struct {
	message proto.Message
	err     error
}

// result adapts an RPC's (response, error) pair for respond.
func result[T proto.Message](message T, err error) outcome { return outcome{message, err} }

func respond(w http.ResponseWriter, o outcome) {
	message, err := o.message, o.err
	if err != nil {
		writeError(w, err)
		return
	}
	data, err := jsonOut.Marshal(message)
	if err != nil {
		writeError(w, status.Error(codes.Internal, err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// respondList writes a streaming RPC's responses as a JSON array, as the REST API does.
func respondList[T proto.Message](w http.ResponseWriter, messages []T, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	var out bytes.Buffer
	out.WriteByte('[')
	for i, m := range messages {
		data, err := jsonOut.Marshal(m)
		if err != nil {
			writeError(w, status.Error(codes.Internal, err.Error()))
			return
		}
		if i > 0 {
			out.WriteByte(',')
		}
		out.Write(data)
	}
	out.WriteByte(']')
	w.Header().Set("Content-Type", "application/json")
	w.Write(out.Bytes())
}

var httpStatus = map[codes.Code]int{
	codes.InvalidArgument: 400, codes.FailedPrecondition: 400, codes.OutOfRange: 400,
	codes.Unauthenticated: 401, codes.PermissionDenied: 403, codes.NotFound: 404,
	codes.AlreadyExists: 409, codes.Aborted: 409, codes.ResourceExhausted: 429,
	codes.Canceled: 499, codes.Unimplemented: 501, codes.Unavailable: 503, codes.DeadlineExceeded: 504,
}

var canonicalName = map[codes.Code]string{
	codes.Canceled: "CANCELLED", codes.Unknown: "UNKNOWN", codes.InvalidArgument: "INVALID_ARGUMENT",
	codes.DeadlineExceeded: "DEADLINE_EXCEEDED", codes.NotFound: "NOT_FOUND", codes.AlreadyExists: "ALREADY_EXISTS",
	codes.PermissionDenied: "PERMISSION_DENIED", codes.ResourceExhausted: "RESOURCE_EXHAUSTED",
	codes.FailedPrecondition: "FAILED_PRECONDITION", codes.Aborted: "ABORTED", codes.OutOfRange: "OUT_OF_RANGE",
	codes.Unimplemented: "UNIMPLEMENTED", codes.Internal: "INTERNAL", codes.Unavailable: "UNAVAILABLE",
	codes.DataLoss: "DATA_LOSS", codes.Unauthenticated: "UNAUTHENTICATED",
}

func writeError(w http.ResponseWriter, err error) {
	st := status.Convert(err)
	code, ok := httpStatus[st.Code()]
	if !ok {
		code = 500
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "message": st.Message(), "status": canonicalName[st.Code()]}})
}
