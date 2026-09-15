// Command docket-server exposes a Docket queue over gRPC so that services in
// any language can enqueue and inspect jobs without database access, and
// serves a small web dashboard.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/varshakodi/docket/internal/api"
	"github.com/varshakodi/docket/internal/dashboard"
	"github.com/varshakodi/docket/internal/store"
)

func main() {
	addr := flag.String("addr", ":50051", "gRPC address to listen on")
	httpAddr := flag.String("http", ":8080", "dashboard address (empty to disable)")
	flag.Parse()

	if err := run(*addr, *httpAddr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(addr, httpAddr string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	s, err := store.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer s.Close()

	if httpAddr != "" {
		go dashboard.Serve(ctx, httpAddr, s, log)
	}
	return api.Serve(ctx, addr, s, log)
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://localhost:5432/docket?sslmode=disable"
}
