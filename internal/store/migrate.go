package store

import (
	"database/sql"
	"fmt"

	// Registers the "pgx" driver with database/sql. The underscore means we
	// import it only for that side effect, never calling it directly.
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/varshakodi/docket/migrations"
)

// Migrate applies or rolls back schema migrations.
//
// goose needs a database/sql handle rather than a pgx pool, so we open a
// short-lived one here. The application itself uses pgxpool (see pool.go).
func Migrate(dsn, direction string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}

	switch direction {
	case "up":
		return goose.Up(db, ".")
	case "down":
		return goose.Down(db, ".")
	case "status":
		return goose.Status(db, ".")
	default:
		return fmt.Errorf("unknown direction %q (want up, down or status)", direction)
	}
}
