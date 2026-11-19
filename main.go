// Command firestore-emulator is a small Firestore emulator for local development.
//
// It serves the Firestore v1 gRPC and REST APIs on one port for any number of projects and
// databases, keeps only the current version of each document, and reads and writes the Cloud
// Firestore export format used by the Java emulator's :export and :import endpoints.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
)

func main() {
	host := flag.String("host", "127.0.0.1", "address to listen on")
	port := flag.Int("port", 8080, "port serving both gRPC and REST")
	flag.Parse()
	if os.Getenv("GOGC") == "" {
		// Favor a smaller heap for local development.
		debug.SetGCPercent(50)
	}

	server := &Server{store: NewStore()}
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20))
	pb.RegisterFirestoreServer(grpcServer, server)
	rest := &restHandler{server: server}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}
		rest.ServeHTTP(w, r)
	})
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true) // gRPC clients connect with HTTP/2 prior knowledge
	httpServer := &http.Server{Handler: handler, Protocols: protocols, ReadHeaderTimeout: 30 * time.Second}

	address := net.JoinHostPort(*host, fmt.Sprint(*port))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		httpServer.Shutdown(ctx)
	}()
	log.Printf("Firestore emulator listening on %s (gRPC and REST)", address)
	if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
