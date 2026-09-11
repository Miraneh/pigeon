package main

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

const expiry = 3 * time.Hour

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

func identityBalanceOf(t *testing.T, db *gorm.DB, id int64) int64 {
	t.Helper()
	var balance int64
	if err := db.Raw(`SELECT balance FROM identities WHERE id = ?`, id).Scan(&balance).Error; err != nil {
		t.Fatalf("read balance for identity %d: %v", id, err)
	}
	return balance
}

func balanceLockExists(t *testing.T, db *gorm.DB, identityID int64, hourBucket time.Time) bool {
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

func TestReap_RefundsStaleLockAndClearsRow(t *testing.T) {
	db := openTestDB(t)
	seedIdentity(t, db, 1, 50)

	hourBucket := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Hour)
	seedBalanceLock(t, db, 1, hourBucket, 30)

	refunded, err := reap(context.Background(), db, expiry)
	if err != nil {
		t.Fatalf("reap() error = %v", err)
	}

	if refunded != 1 {
		t.Errorf("refunded = %d, want 1", refunded)
	}
	if got := identityBalanceOf(t, db, 1); got != 80 {
		t.Errorf("identity balance = %d, want 80", got)
	}
	if balanceLockExists(t, db, 1, hourBucket) {
		t.Error("balance_locks row still exists after reap, want it cleared")
	}
}

func TestReap_IgnoresRecentLocks(t *testing.T) {
	db := openTestDB(t)
	seedIdentity(t, db, 1, 50)

	hourBucket := time.Now().UTC().Truncate(time.Hour)
	seedBalanceLock(t, db, 1, hourBucket, 30)

	refunded, err := reap(context.Background(), db, expiry)
	if err != nil {
		t.Fatalf("reap() error = %v", err)
	}

	if refunded != 0 {
		t.Errorf("refunded = %d, want 0", refunded)
	}
	if got := identityBalanceOf(t, db, 1); got != 50 {
		t.Errorf("identity balance = %d, want 50 (untouched)", got)
	}
	if !balanceLockExists(t, db, 1, hourBucket) {
		t.Error("balance_locks row was cleared, want it left alone (not yet stale)")
	}
}

func TestReap_ClearsZeroAmountStaleLockWithoutCounting(t *testing.T) {
	db := openTestDB(t)
	seedIdentity(t, db, 1, 50)

	hourBucket := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Hour)
	seedBalanceLock(t, db, 1, hourBucket, 0) // fully delivered and finalized before it went stale

	refunded, err := reap(context.Background(), db, expiry)
	if err != nil {
		t.Fatalf("reap() error = %v", err)
	}

	if refunded != 0 {
		t.Errorf("refunded = %d, want 0 (nothing owed)", refunded)
	}
	if got := identityBalanceOf(t, db, 1); got != 50 {
		t.Errorf("identity balance = %d, want 50 (untouched)", got)
	}
	if balanceLockExists(t, db, 1, hourBucket) {
		t.Error("balance_locks row still exists after reap, want it cleared")
	}
}

func TestReap_HandlesMultipleIdentitiesIndependently(t *testing.T) {
	db := openTestDB(t)
	seedIdentity(t, db, 1, 0)
	seedIdentity(t, db, 2, 0)
	seedIdentity(t, db, 3, 0)

	stale := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Hour)
	fresh := time.Now().UTC().Truncate(time.Hour)

	seedBalanceLock(t, db, 1, stale, 10)
	seedBalanceLock(t, db, 2, stale, 20)
	seedBalanceLock(t, db, 3, fresh, 30) // not stale yet

	refunded, err := reap(context.Background(), db, expiry)
	if err != nil {
		t.Fatalf("reap() error = %v", err)
	}

	if refunded != 2 {
		t.Errorf("refunded = %d, want 2", refunded)
	}
	if got := identityBalanceOf(t, db, 1); got != 10 {
		t.Errorf("identity 1 balance = %d, want 10", got)
	}
	if got := identityBalanceOf(t, db, 2); got != 20 {
		t.Errorf("identity 2 balance = %d, want 20", got)
	}
	if got := identityBalanceOf(t, db, 3); got != 0 {
		t.Errorf("identity 3 balance = %d, want 0 (lock not yet stale)", got)
	}
	if !balanceLockExists(t, db, 3, fresh) {
		t.Error("identity 3's fresh lock was cleared, want it left alone")
	}
}

func TestRefundLock_ReturnsFalseAndLeavesZeroBalanceUntouched(t *testing.T) {
	db := openTestDB(t)
	seedIdentity(t, db, 1, 0)

	hourBucket := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Hour)
	seedBalanceLock(t, db, 1, hourBucket, 0)

	ok, err := refundLock(context.Background(), db, 1, hourBucket)
	if err != nil {
		t.Fatalf("refundLock() error = %v", err)
	}
	if ok {
		t.Error("refundLock() = true, want false for a zero-amount lock")
	}
	if balanceLockExists(t, db, 1, hourBucket) {
		t.Error("balance_locks row still exists after refundLock, want it cleared")
	}
}
