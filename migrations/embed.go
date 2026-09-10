// Package migrations embeds the SQL migration files into the compiled binary.
//
// go:embed can only reference files at or below its own directory, which is
// why this tiny package lives here rather than in internal/store.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
