package main

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Integration tests in this package need a real Postgres instance. They skip themselves when one isn't reachable.
// `docker compose -f production/docker-compose.yml up -d db` starts one with matching credentials.

var (
	migrateTestDBOnce sync.Once
	migrateTestDBErr  error
)

func testDSN() string {
	if v := os.Getenv("TEST_DATABASE_DSN"); v != "" {
		return v
	}
	return "host=localhost port=5432 user=pigeon password=pigeon dbname=pigeon sslmode=disable"
}

func testDatabaseURL() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://pigeon:pigeon@localhost:5432/pigeon?sslmode=disable"
}

// openTestDB connects to the test Postgres instance, applies migrations once per test binary run, truncates the
// pipeline tables, and returns a ready-to-use *gorm.DB. It skips the calling test when no database is reachable.
func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(postgres.Open(testDSN()), &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)})
	if err != nil {
		t.Skipf("test postgres not reachable: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Skipf("test postgres not reachable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Skipf("test postgres not reachable: %v", err)
	}

	migrateTestDBOnce.Do(func() {
		migrateTestDBErr = runMigrations(testDatabaseURL())
	})
	if migrateTestDBErr != nil {
		t.Fatalf("run migrations: %v", migrateTestDBErr)
	}

	stmt := `TRUNCATE TABLE report_stats, balance_locks, identities RESTART IDENTITY CASCADE`
	if err := db.Exec(stmt).Error; err != nil {
		t.Fatalf("truncate test tables: %v", err)
	}

	return db
}
