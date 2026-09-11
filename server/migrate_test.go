package main

import "testing"

func TestRunMigrations_InvalidURLReturnsError(t *testing.T) {
	if err := runMigrations("not-a-real-scheme://nowhere"); err == nil {
		t.Fatal("runMigrations() with an invalid URL, want error, got nil")
	}
}

func TestRunMigrations_AppliesSchemaAndIsIdempotent(t *testing.T) {
	db := openTestDB(t) // already applies migrations once for this test binary run

	var tableCount int64
	err := db.Raw(
		`SELECT count(*) FROM information_schema.tables WHERE table_name IN ('identities', 'balance_locks')`,
	).Scan(&tableCount).Error
	if err != nil {
		t.Fatalf("query information_schema: %v", err)
	}
	if tableCount != 2 {
		t.Fatalf("found %d of the expected tables, want 2", tableCount)
	}

	if err := runMigrations(testDatabaseURL()); err != nil {
		t.Fatalf("re-running runMigrations() on an up-to-date schema: %v", err)
	}
}
