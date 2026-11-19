package main

import (
	"context"
	"sort"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server implements the Firestore v1 API over the store. Listen and Write (realtime streams) and
// PartitionQuery are not implemented.
type Server struct {
	pb.UnimplementedFirestoreServer
	store *Store
}

// readTransaction resolves the transaction a read participates in, starting one when asked.
func (s *Server) readTransaction(db string, existing []byte, begin *pb.TransactionOptions) (*transaction, []byte, error) {
	if begin != nil {
		id := s.store.beginTransaction(db, begin)
		tx, err := s.store.transaction(db, id)
		return tx, id, err
	}
	if len(existing) > 0 {
		tx, err := s.store.transaction(db, existing)
		return tx, nil, err
	}
	return nil, nil, nil
}

func (s *Server) GetDocument(_ context.Context, req *pb.GetDocumentRequest) (*pb.Document, error) {
	dbName, path, err := parseDocumentName(req.GetName())
	if err != nil {
		return nil, err
	}
	tx, _, err := s.readTransaction(dbName, req.GetTransaction(), nil)
	if err != nil {
		return nil, err
	}
	db := s.store.database(dbName)
	db.mu.RLock()
	defer db.mu.RUnlock()
	e := db.docs[path]
	s.store.recordRead(tx, path, e)
	if e == nil {
		return nil, status.Errorf(codes.NotFound, "Document %q not found", req.GetName())
	}
	return db.document(e, req.GetMask())
}

func (s *Server) ListDocuments(_ context.Context, req *pb.ListDocumentsRequest) (*pb.ListDocumentsResponse, error) {
	dbName, parent, err := parseName(req.GetParent())
	if err != nil {
		return nil, err
	}
	collection := req.GetCollectionId()
	if parent != "" {
		collection = parent + "/" + collection
	}
	db := s.store.database(dbName)
	db.mu.RLock()
	defer db.mu.RUnlock()
	type item struct {
		id string
		e  *entry
	}
	var items []item
	for id, e := range db.collections[collection] {
		items = append(items, item{id, e})
	}
	if req.GetShowMissing() {
		for _, id := range db.missingParents(collection) {
			items = append(items, item{id, nil})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].id < items[j].id })
	if token := req.GetPageToken(); token != "" {
		i := sort.Search(len(items), func(i int) bool { return items[i].id > token })
		items = items[i:]
	}
	response := &pb.ListDocumentsResponse{}
	if size := int(req.GetPageSize()); size > 0 && len(items) > size {
		items = items[:size]
		response.NextPageToken = items[size-1].id
	}
	for _, it := range items {
		if it.e == nil {
			response.Documents = append(response.Documents, &pb.Document{Name: db.documentName(collection + "/" + it.id)})
			continue
		}
		doc, err := db.document(it.e, req.GetMask())
		if err != nil {
			return nil, err
		}
		response.Documents = append(response.Documents, doc)
	}
	return response, nil
}

func (s *Server) writeOne(name string, w *pb.Write, mask *pb.DocumentMask) (*pb.Document, error) {
	dbName, path, err := parseDocumentName(name)
	if err != nil {
		return nil, err
	}
	if _, err := s.store.commit(dbName, []*pb.Write{w}, nil); err != nil {
		return nil, err
	}
	db := s.store.database(dbName)
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.document(db.docs[path], mask)
}

func (s *Server) UpdateDocument(_ context.Context, req *pb.UpdateDocumentRequest) (*pb.Document, error) {
	w := &pb.Write{Operation: &pb.Write_Update{Update: req.GetDocument()}, UpdateMask: req.GetUpdateMask(), CurrentDocument: req.GetCurrentDocument()}
	return s.writeOne(req.GetDocument().GetName(), w, req.GetMask())
}

func (s *Server) CreateDocument(_ context.Context, req *pb.CreateDocumentRequest) (*pb.Document, error) {
	id := req.GetDocumentId()
	if id == "" {
		id = newID(20)
	}
	doc := &pb.Document{Name: req.GetParent() + "/" + req.GetCollectionId() + "/" + id, Fields: req.GetDocument().GetFields()}
	w := &pb.Write{Operation: &pb.Write_Update{Update: doc}, CurrentDocument: &pb.Precondition{ConditionType: &pb.Precondition_Exists{Exists: false}}}
	return s.writeOne(doc.Name, w, req.GetMask())
}

func (s *Server) DeleteDocument(_ context.Context, req *pb.DeleteDocumentRequest) (*emptypb.Empty, error) {
	dbName, _, err := parseDocumentName(req.GetName())
	if err != nil {
		return nil, err
	}
	w := &pb.Write{Operation: &pb.Write_Delete{Delete: req.GetName()}, CurrentDocument: req.GetCurrentDocument()}
	if _, err := s.store.commit(dbName, []*pb.Write{w}, nil); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) BatchGetDocuments(req *pb.BatchGetDocumentsRequest, stream pb.Firestore_BatchGetDocumentsServer) error {
	responses, err := s.batchGet(req)
	if err != nil {
		return err
	}
	for _, r := range responses {
		if err := stream.Send(r); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) batchGet(req *pb.BatchGetDocumentsRequest) ([]*pb.BatchGetDocumentsResponse, error) {
	tx, began, err := s.readTransaction(req.GetDatabase(), req.GetTransaction(), req.GetNewTransaction())
	if err != nil {
		return nil, err
	}
	db := s.store.database(req.GetDatabase())
	db.mu.RLock()
	defer db.mu.RUnlock()
	readTime := timestamppb.Now()
	var responses []*pb.BatchGetDocumentsResponse
	for _, name := range req.GetDocuments() {
		dbName, path, err := parseDocumentName(name)
		if err != nil {
			return nil, err
		}
		if dbName != req.GetDatabase() {
			return nil, status.Errorf(codes.InvalidArgument, "%q is not in database %q", name, req.GetDatabase())
		}
		e := db.docs[path]
		s.store.recordRead(tx, path, e)
		r := &pb.BatchGetDocumentsResponse{ReadTime: readTime}
		if e == nil {
			r.Result = &pb.BatchGetDocumentsResponse_Missing{Missing: name}
		} else {
			doc, err := db.document(e, req.GetMask())
			if err != nil {
				return nil, err
			}
			r.Result = &pb.BatchGetDocumentsResponse_Found{Found: doc}
		}
		responses = append(responses, r)
	}
	if began != nil {
		if len(responses) == 0 {
			responses = append(responses, &pb.BatchGetDocumentsResponse{ReadTime: readTime})
		}
		responses[0].Transaction = began
	}
	return responses, nil
}

func (s *Server) BeginTransaction(_ context.Context, req *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	return &pb.BeginTransactionResponse{Transaction: s.store.beginTransaction(req.GetDatabase(), req.GetOptions())}, nil
}

func (s *Server) Commit(_ context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	return s.store.commit(req.GetDatabase(), req.GetWrites(), req.GetTransaction())
}

// Rollback is idempotent: clients roll back after a failed commit, which has already ended it.
func (s *Server) Rollback(_ context.Context, req *pb.RollbackRequest) (*emptypb.Empty, error) {
	s.store.endTransaction(req.GetTransaction())
	return &emptypb.Empty{}, nil
}

func (s *Server) BatchWrite(_ context.Context, req *pb.BatchWriteRequest) (*pb.BatchWriteResponse, error) {
	return s.store.batchWrite(req.GetDatabase(), req.GetWrites()), nil
}

func (s *Server) RunQuery(req *pb.RunQueryRequest, stream pb.Firestore_RunQueryServer) error {
	responses, err := s.runQuery(req)
	if err != nil {
		return err
	}
	for _, r := range responses {
		if err := stream.Send(r); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) runQuery(req *pb.RunQueryRequest) ([]*pb.RunQueryResponse, error) {
	dbName, parent, err := parseName(req.GetParent())
	if err != nil {
		return nil, err
	}
	tx, began, err := s.readTransaction(dbName, req.GetTransaction(), req.GetNewTransaction())
	if err != nil {
		return nil, err
	}
	db := s.store.database(dbName)
	db.mu.RLock()
	defer db.mu.RUnlock()
	rows, err := db.runQuery(parent, req.GetStructuredQuery())
	if err != nil {
		return nil, err
	}
	s.store.recordQuery(tx, parent, req.GetStructuredQuery(), rows)
	readTime := timestamppb.Now()
	responses := make([]*pb.RunQueryResponse, 0, len(rows)+1)
	for _, r := range rows {
		s.store.recordRead(tx, r.e.path, r.e)
		responses = append(responses, &pb.RunQueryResponse{ReadTime: readTime, Document: &pb.Document{
			Name: db.documentName(r.e.path), Fields: r.fields, CreateTime: micros(r.e.create), UpdateTime: micros(r.e.update)}})
	}
	// An empty result still reports its read time.
	if len(responses) == 0 {
		responses = append(responses, &pb.RunQueryResponse{ReadTime: readTime})
	}
	responses[0].Transaction = began
	return responses, nil
}

func (s *Server) RunAggregationQuery(req *pb.RunAggregationQueryRequest, stream pb.Firestore_RunAggregationQueryServer) error {
	response, err := s.runAggregationQuery(req)
	if err != nil {
		return err
	}
	return stream.Send(response)
}

func (s *Server) runAggregationQuery(req *pb.RunAggregationQueryRequest) (*pb.RunAggregationQueryResponse, error) {
	dbName, parent, err := parseName(req.GetParent())
	if err != nil {
		return nil, err
	}
	tx, began, err := s.readTransaction(dbName, req.GetTransaction(), req.GetNewTransaction())
	if err != nil {
		return nil, err
	}
	query := req.GetStructuredAggregationQuery()
	db := s.store.database(dbName)
	db.mu.RLock()
	defer db.mu.RUnlock()
	rows, err := db.runQuery(parent, query.GetStructuredQuery())
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		s.store.recordRead(tx, r.e.path, r.e)
	}
	s.store.recordQuery(tx, parent, query.GetStructuredQuery(), rows)
	fields, err := aggregate(rows, query.GetAggregations())
	if err != nil {
		return nil, err
	}
	return &pb.RunAggregationQueryResponse{Result: &pb.AggregationResult{AggregateFields: fields}, ReadTime: timestamppb.Now(), Transaction: began}, nil
}

func (s *Server) ListCollectionIds(_ context.Context, req *pb.ListCollectionIdsRequest) (*pb.ListCollectionIdsResponse, error) {
	dbName, parent, err := parseName(req.GetParent())
	if err != nil {
		return nil, err
	}
	db := s.store.database(dbName)
	db.mu.RLock()
	ids := db.childCollections(parent)
	db.mu.RUnlock()
	if token := req.GetPageToken(); token != "" {
		ids = ids[sort.SearchStrings(ids, token+"\x00"):]
	}
	response := &pb.ListCollectionIdsResponse{CollectionIds: ids}
	if size := int(req.GetPageSize()); size > 0 && len(ids) > size {
		response.CollectionIds = ids[:size]
		response.NextPageToken = ids[size-1]
	}
	return response, nil
}
