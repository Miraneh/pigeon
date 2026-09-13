// Package dbtest runs a throwaway Postgres container for tests.
package dbtest

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

	"pigeon/internal/database"
)

var (
	testDBURL     string
	testDBSkipMsg string
)

// Main starts the Postgres test container, applies migrations, and runs m. Call it from a package's TestMain.
func Main(m *testing.M) int {
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

	if err := database.RunMigrations(testDBURL); err != nil {
		fmt.Fprintf(os.Stderr, "run migrations against test container: %v\n", err)
		return 1
	}

	return m.Run()
}

func URL() string {
	return testDBURL
}

func Open(t *testing.T) *gorm.DB {
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

func SeedIdentity(t *testing.T, db *gorm.DB, id, balance int64) {
	t.Helper()
	if err := db.Exec(`INSERT INTO identities (id, balance) VALUES (?, ?)`, id, balance).Error; err != nil {
		t.Fatalf("seed identity %d: %v", id, err)
	}
}

func SeedBalanceLock(t *testing.T, db *gorm.DB, identityID int64, hourBucket time.Time, amount int64) {
	t.Helper()
	err := db.Exec(
		`INSERT INTO balance_locks (identity_id, hour_bucket, amount) VALUES (?, ?, ?)`,
		identityID, hourBucket, amount,
	).Error
	if err != nil {
		t.Fatalf("seed balance lock for identity %d: %v", identityID, err)
	}
}

func SeedReportStats(
	t *testing.T, db *gorm.DB, identityID int64, hourBucket time.Time, messagesSent, amountSpent int64,
) {
	t.Helper()
	err := db.Exec(
		`INSERT INTO report_stats (identity_id, hour_bucket, messages_sent, amount_spent) VALUES (?, ?, ?, ?)`,
		identityID, hourBucket, messagesSent, amountSpent,
	).Error
	if err != nil {
		t.Fatalf("seed report stats for identity %d: %v", identityID, err)
	}
}

func IdentityBalance(t *testing.T, db *gorm.DB, id int64) int64 {
	t.Helper()
	var balance int64
	if err := db.Raw(`SELECT balance FROM identities WHERE id = ?`, id).Scan(&balance).Error; err != nil {
		t.Fatalf("read balance for identity %d: %v", id, err)
	}
	return balance
}

func BalanceLockAmount(t *testing.T, db *gorm.DB, identityID int64, hourBucket time.Time) int64 {
	t.Helper()
	var amount int64
	err := db.Raw(
		`SELECT amount FROM balance_locks WHERE identity_id = ? AND hour_bucket = ?`,
		identityID, hourBucket,
	).Scan(&amount).Error
	if err != nil {
		t.Fatalf("read balance lock for identity %d: %v", identityID, err)
	}
	return amount
}

func BalanceLockExists(t *testing.T, db *gorm.DB, identityID int64, hourBucket time.Time) bool {
	t.Helper()
	var count int64
	err := db.Raw(
		`SELECT count(*) FROM balance_locks WHERE identity_id = ? AND hour_bucket = ?`,
		identityID, hourBucket,
	).Scan(&count).Error
	if err != nil {
		t.Fatalf("check balance lock existence for identity %d: %v", identityID, err)
	}
	return count > 0
}

func ReportStats(
	t *testing.T, db *gorm.DB, identityID int64, hourBucket time.Time,
) (messagesSent, amountSpent int64) {
	t.Helper()
	err := db.Raw(
		`SELECT messages_sent, amount_spent FROM report_stats WHERE identity_id = ? AND hour_bucket = ?`,
		identityID, hourBucket,
	).Row().Scan(&messagesSent, &amountSpent)
	if err != nil {
		t.Fatalf("read report stats for identity %d: %v", identityID, err)
	}
	return messagesSent, amountSpent
}
