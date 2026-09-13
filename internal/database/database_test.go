package database_test

import (
	"flag"
	"os"
	"testing"

	"pigeon/internal/database"
	"pigeon/internal/dbtest"
)

func TestMain(m *testing.M) {
	os.Exit(dbtest.Main(m))
}

func parseConfig(t *testing.T, args ...string) *database.Config {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := database.RegisterFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return c
}

var testArgs = []string{
	"-db-host=dbhost", "-db-port=1111", "-db-user=u", "-db-password=p", "-db-name=n", "-db-sslmode=require",
}

func TestConfig_DSN(t *testing.T) {
	c := parseConfig(t, testArgs...)

	want := "host=dbhost port=1111 user=u password=p dbname=n sslmode=require"
	if got := c.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}

func TestConfig_URL(t *testing.T) {
	c := parseConfig(t, testArgs...)

	want := "postgres://u:p@dbhost:1111/n?sslmode=require" //nolint:gosec // fake fixture credentials, not real ones
	if got := c.URL(); got != want {
		t.Errorf("URL() = %q, want %q", got, want)
	}
}
