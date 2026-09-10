// Command docket is the command-line interface to a Docket job queue.
package main

import (
	"fmt"
	"os"

	"github.com/varshakodi/docket/internal/store"
)

const usage = `docket - a durable job queue on PostgreSQL

Usage:
  docket migrate up       apply all pending migrations
  docket migrate down     roll back the most recent migration
  docket migrate status   show which migrations have been applied

Environment:
  DATABASE_URL   PostgreSQL connection string
                 (default: postgres://localhost:5432/docket?sslmode=disable)
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}

	switch args[0] {
	case "migrate":
		if len(args) < 2 {
			return fmt.Errorf("migrate needs a direction: up, down or status")
		}
		return store.Migrate(databaseURL(), args[1])
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	default:
		return fmt.Errorf("unknown command %q (try: docket help)", args[0])
	}
}

// databaseURL reads the connection string from the environment, falling back
// to a local development default so the tool works with no setup.
func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://localhost:5432/docket?sslmode=disable"
}
