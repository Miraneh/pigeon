// Package migrations embeds the SQL migration files into the binary so the container image
// doesn't need the migrations directory copied separately.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
