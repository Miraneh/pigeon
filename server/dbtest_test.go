package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

var (
	testDBURL     string
	testDBSkipMsg string
)

func TestMain(m *testing.M) {
	os.Exit(runTestMain(m))
}

func runTestMain(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("pigeon"),
		tcpostgres.WithUsername("pigeon"),
		tcpostgres.WithPassword("pigeon"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp").WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		testDBSkipMsg = fmt.Sprintf("could not start postgres test container (is Docker running?): %v", err)
		return m.Run()
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres test container: %v\n", err)
		}
	}()

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		testDBSkipMsg = fmt.Sprintf("get postgres test container connection string: %v", err)
		return m.Run()
	}
	testDBURL = connStr

	if err := runMigrations(testDBURL); err != nil {
		fmt.Fprintf(os.Stderr, "run migrations against test container: %v\n", err)
		return 1
	}

	return m.Run()
}

func testDatabaseURL() string {
	return testDBURL
}

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	if testDBSkipMsg != "" {
		t.Skip(testDBSkipMsg)
	}

	db, err := gorm.Open(postgres.Open(testDBURL), &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)})
	if err != nil {
		t.Fatalf("connect to test postgres: %v", err)
	}

	stmt := `TRUNCATE TABLE report_stats, balance_locks, identities RESTART IDENTITY CASCADE`
	if err := db.Exec(stmt).Error; err != nil {
		t.Fatalf("truncate test tables: %v", err)
	}

	return db
}
