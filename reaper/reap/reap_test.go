package reap

import (
	"context"
	"os"
	"testing"
	"time"

	"pigeon/internal/dbtest"
)

const expiry = 3 * time.Hour

func TestMain(m *testing.M) {
	os.Exit(dbtest.Main(m))
}

func TestRun_RefundsStaleLockAndClearsRow(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 50)

	hourBucket := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Hour)
	dbtest.SeedBalanceLock(t, db, 1, hourBucket, 30)

	refunded, err := Run(context.Background(), db, expiry)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if refunded != 1 {
		t.Errorf("refunded = %d, want 1", refunded)
	}
	if got := dbtest.IdentityBalance(t, db, 1); got != 80 {
		t.Errorf("identity balance = %d, want 80", got)
	}
	if dbtest.BalanceLockExists(t, db, 1, hourBucket) {
		t.Error("balance_locks row still exists after reap, want it cleared")
	}
}

func TestRun_IgnoresRecentLocks(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 50)

	hourBucket := time.Now().UTC().Truncate(time.Hour)
	dbtest.SeedBalanceLock(t, db, 1, hourBucket, 30)

	refunded, err := Run(context.Background(), db, expiry)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if refunded != 0 {
		t.Errorf("refunded = %d, want 0", refunded)
	}
	if got := dbtest.IdentityBalance(t, db, 1); got != 50 {
		t.Errorf("identity balance = %d, want 50 (untouched)", got)
	}
	if !dbtest.BalanceLockExists(t, db, 1, hourBucket) {
		t.Error("balance_locks row was cleared, want it left alone (not yet stale)")
	}
}

func TestRun_ClearsZeroAmountStaleLockWithoutCounting(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 50)

	hourBucket := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Hour)
	dbtest.SeedBalanceLock(t, db, 1, hourBucket, 0) // fully delivered and finalized before it went stale

	refunded, err := Run(context.Background(), db, expiry)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if refunded != 0 {
		t.Errorf("refunded = %d, want 0 (nothing owed)", refunded)
	}
	if got := dbtest.IdentityBalance(t, db, 1); got != 50 {
		t.Errorf("identity balance = %d, want 50 (untouched)", got)
	}
	if dbtest.BalanceLockExists(t, db, 1, hourBucket) {
		t.Error("balance_locks row still exists after reap, want it cleared")
	}
}

func TestRun_HandlesMultipleIdentitiesIndependently(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 0)
	dbtest.SeedIdentity(t, db, 2, 0)
	dbtest.SeedIdentity(t, db, 3, 0)

	stale := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Hour)
	fresh := time.Now().UTC().Truncate(time.Hour)

	dbtest.SeedBalanceLock(t, db, 1, stale, 10)
	dbtest.SeedBalanceLock(t, db, 2, stale, 20)
	dbtest.SeedBalanceLock(t, db, 3, fresh, 30) // not stale yet

	refunded, err := Run(context.Background(), db, expiry)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if refunded != 2 {
		t.Errorf("refunded = %d, want 2", refunded)
	}
	if got := dbtest.IdentityBalance(t, db, 1); got != 10 {
		t.Errorf("identity 1 balance = %d, want 10", got)
	}
	if got := dbtest.IdentityBalance(t, db, 2); got != 20 {
		t.Errorf("identity 2 balance = %d, want 20", got)
	}
	if got := dbtest.IdentityBalance(t, db, 3); got != 0 {
		t.Errorf("identity 3 balance = %d, want 0 (lock not yet stale)", got)
	}
	if !dbtest.BalanceLockExists(t, db, 3, fresh) {
		t.Error("identity 3's fresh lock was cleared, want it left alone")
	}
}

func TestRefundLock_ReturnsFalseAndLeavesZeroBalanceUntouched(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 0)

	hourBucket := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Hour)
	dbtest.SeedBalanceLock(t, db, 1, hourBucket, 0)

	ok, err := refundLock(context.Background(), db, 1, hourBucket)
	if err != nil {
		t.Fatalf("refundLock() error = %v", err)
	}
	if ok {
		t.Error("refundLock() = true, want false for a zero-amount lock")
	}
	if dbtest.BalanceLockExists(t, db, 1, hourBucket) {
		t.Error("balance_locks row still exists after refundLock, want it cleared")
	}
}
