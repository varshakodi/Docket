// Package docketv1 holds the gRPC contract for Docket and the Go code
// generated from it.
//
// docket.proto is the source of truth. The *.pb.go files are generated and
// committed, so building the project needs no protoc. To regenerate after
// editing the .proto:
//
//	go generate ./api/...
//
// which requires protoc, protoc-gen-go and protoc-gen-go-grpc on PATH.
package docketv1

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative docket.proto
