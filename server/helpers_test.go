package main

import (
	"testing"
	"time"

	"gorm.io/gorm"
)

// newAdmitterForTest builds an admitter with real channels and a fixed price of 1.
func newAdmitterForTest(db *gorm.DB, window time.Duration, maxSize int) *admitter {
	return &admitter{
		in:        make(chan *admissionRequest, maxSize*4+10),
		window:    window,
		maxSize:   maxSize,
		db:        db,
		price:     1,
		expressCh: make(chan dispatchItem, 100),
		regularCh: make(chan dispatchItem, 100),
	}
}

func seedIdentity(t *testing.T, db *gorm.DB, id, balance int64) {
	t.Helper()
	if err := db.Exec(`INSERT INTO identities (id, balance) VALUES (?, ?)`, id, balance).Error; err != nil {
		t.Fatalf("seed identity %d: %v", id, err)
	}
}

func seedBalanceLock(t *testing.T, db *gorm.DB, identityID int64, hourBucket time.Time, amount int64) {
	t.Helper()
	err := db.Exec(
		`INSERT INTO balance_locks (identity_id, hour_bucket, amount) VALUES (?, ?, ?)`,
		identityID, hourBucket, amount,
	).Error
	if err != nil {
		t.Fatalf("seed balance lock for identity %d: %v", identityID, err)
	}
}

func seedReportStats(
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

func identityBalanceOf(t *testing.T, db *gorm.DB, id int64) int64 {
	t.Helper()
	var balance int64
	if err := db.Raw(`SELECT balance FROM identities WHERE id = ?`, id).Scan(&balance).Error; err != nil {
		t.Fatalf("read balance for identity %d: %v", id, err)
	}
	return balance
}

func balanceLockAmount(t *testing.T, db *gorm.DB, identityID int64, hourBucket time.Time) int64 {
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

func reportStatsOf(
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

// waitFor polls cond until it returns true or timeout elapses, failing the test otherwise. Used for assertions
// against work done in a background goroutine (e.g. an async dispatch flush).
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}
