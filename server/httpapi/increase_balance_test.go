package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"pigeon/internal/dbtest"
)

func doIncreaseBalance(
	t *testing.T, r http.Handler, identityID int64, key string, amount int64,
) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(increaseBalanceRequest{Amount: amount})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	path := fmt.Sprintf("/identities/%d/balance/increase", identityID)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(idempotencyKeyHeader, key)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func decodeIncreaseBalance(t *testing.T, w *httptest.ResponseRecorder) increaseBalanceResponse {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusOK, w.Body.String())
	}
	var resp increaseBalanceResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return resp
}

func balanceIncreaseCount(t *testing.T, db *gorm.DB, identityID int64) int64 {
	t.Helper()
	var count int64
	err := db.Raw(`SELECT count(*) FROM balance_increases WHERE identity_id = ?`, identityID).Scan(&count).Error
	if err != nil {
		t.Fatalf("count balance increases for identity %d: %v", identityID, err)
	}
	return count
}

func TestIdentityIncreaseBalance_CreatesNewIdentity(t *testing.T) {
	db := dbtest.Open(t)
	r := newTestRouter(db)

	resp := decodeIncreaseBalance(t, doIncreaseBalance(t, r, 7, "k1", 50))

	if want := (increaseBalanceResponse{IdentityID: 7, Amount: 50, Balance: 50}); resp != want {
		t.Errorf("response = %+v, want %+v", resp, want)
	}
	if got := dbtest.IdentityBalance(t, db, 7); got != 50 {
		t.Errorf("identity balance = %d, want 50", got)
	}
}

func TestIdentityIncreaseBalance_AddsToExistingBalance(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 7, 100)
	r := newTestRouter(db)

	resp := decodeIncreaseBalance(t, doIncreaseBalance(t, r, 7, "k1", 50))

	if resp.Balance != 150 {
		t.Errorf("response balance = %d, want 150", resp.Balance)
	}
	if got := dbtest.IdentityBalance(t, db, 7); got != 150 {
		t.Errorf("identity balance = %d, want 150", got)
	}
}

func TestIdentityIncreaseBalance_RejectsNonPositiveAmount(t *testing.T) {
	r := newTestRouter(nil)

	if w := doIncreaseBalance(t, r, 7, "k1", 0); w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestIdentityIncreaseBalance_RequiresValidIdempotencyKey(t *testing.T) {
	r := newTestRouter(nil)

	if w := doIncreaseBalance(t, r, 7, "", 50); w.Code != http.StatusBadRequest {
		t.Errorf("missing key: status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	oversized := strings.Repeat("k", maxIdempotencyKeyLen+1)
	if w := doIncreaseBalance(t, r, 7, oversized, 50); w.Code != http.StatusBadRequest {
		t.Errorf("oversized key: status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestIdentityIncreaseBalance_ReplayReturnsOriginalResponseWithoutIncreasingAgain(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 100)
	r := newTestRouter(db)

	first := decodeIncreaseBalance(t, doIncreaseBalance(t, r, 1, "k1", 50))
	decodeIncreaseBalance(t, doIncreaseBalance(t, r, 1, "k2", 10))
	replay := decodeIncreaseBalance(t, doIncreaseBalance(t, r, 1, "k1", 50))

	if replay != first {
		t.Errorf("replay response = %+v, want the original %+v", replay, first)
	}
	if got := dbtest.IdentityBalance(t, db, 1); got != 160 {
		t.Errorf("identity balance = %d, want 160 (replay must not increase again)", got)
	}
	if got := balanceIncreaseCount(t, db, 1); got != 2 {
		t.Errorf("balance_increases rows = %d, want 2", got)
	}
}

func TestIdentityIncreaseBalance_KeyReusedWithDifferentAmountRejected(t *testing.T) {
	db := dbtest.Open(t)
	r := newTestRouter(db)

	decodeIncreaseBalance(t, doIncreaseBalance(t, r, 1, "k1", 50))

	if w := doIncreaseBalance(t, r, 1, "k1", 60); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusUnprocessableEntity)
	}
	if got := dbtest.IdentityBalance(t, db, 1); got != 50 {
		t.Errorf("identity balance = %d, want 50", got)
	}
}

func TestIdentityIncreaseBalance_SameKeyOnDifferentIdentitiesIncreasesBoth(t *testing.T) {
	db := dbtest.Open(t)
	r := newTestRouter(db)

	decodeIncreaseBalance(t, doIncreaseBalance(t, r, 1, "k1", 50))
	decodeIncreaseBalance(t, doIncreaseBalance(t, r, 2, "k1", 50))

	if got := dbtest.IdentityBalance(t, db, 1); got != 50 {
		t.Errorf("identity 1 balance = %d, want 50", got)
	}
	if got := dbtest.IdentityBalance(t, db, 2); got != 50 {
		t.Errorf("identity 2 balance = %d, want 50", got)
	}
}

func TestIdentityIncreaseBalance_OverflowRejectedAndRolledBack(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, math.MaxInt64-10)
	r := newTestRouter(db)

	if w := doIncreaseBalance(t, r, 1, "k1", 50); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusUnprocessableEntity, w.Body.String())
	}
	if got := dbtest.IdentityBalance(t, db, 1); got != math.MaxInt64-10 {
		t.Errorf("identity balance = %d, want it unchanged", got)
	}
	if got := balanceIncreaseCount(t, db, 1); got != 0 {
		t.Errorf("balance_increases rows = %d, want 0 (failed increase must not consume the key)", got)
	}

	resp := decodeIncreaseBalance(t, doIncreaseBalance(t, r, 1, "k1", 10))
	if resp.Balance != math.MaxInt64 {
		t.Errorf("retry balance = %d, want %d", resp.Balance, int64(math.MaxInt64))
	}
}

func TestIncreaseBalance_ConcurrentSameKeyAppliesOnce(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 100)

	want := increaseBalanceResponse{IdentityID: 1, Amount: 50, Balance: 150}

	// Hold the identity row so every request queues up behind it and they all race once it's released.
	tx := db.Begin()
	if err := tx.Exec(`SELECT balance FROM identities WHERE id = 1 FOR UPDATE`).Error; err != nil {
		t.Fatalf("lock identity: %v", err)
	}

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := increaseBalance(context.Background(), db, 1, "k1", 50)
			if err != nil {
				t.Errorf("increaseBalance() error = %v", err)
				return
			}
			if res != want {
				t.Errorf("increaseBalance() = %+v, want %+v", res, want)
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	if err := tx.Commit().Error; err != nil {
		t.Fatalf("commit: %v", err)
	}
	wg.Wait()

	if got := dbtest.IdentityBalance(t, db, 1); got != 150 {
		t.Errorf("identity balance = %d, want 150 (increased exactly once)", got)
	}
	if got := balanceIncreaseCount(t, db, 1); got != 1 {
		t.Errorf("balance_increases rows = %d, want 1", got)
	}
}

func TestIncreaseBalance_ConcurrentDistinctKeysAllApplied(t *testing.T) {
	db := dbtest.Open(t)

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := increaseBalance(context.Background(), db, 1, fmt.Sprintf("k%d", i), 5); err != nil {
				t.Errorf("increaseBalance() error = %v", err)
			}
		}()
	}
	wg.Wait()

	if got := dbtest.IdentityBalance(t, db, 1); got != 100 {
		t.Errorf("identity balance = %d, want 100 (no lost updates)", got)
	}
	if got := balanceIncreaseCount(t, db, 1); got != 20 {
		t.Errorf("balance_increases rows = %d, want 20", got)
	}
}

func TestIncreaseBalance_WaitsForConcurrentBalanceLock(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 100)

	// Hold the identity row the way admission's lockBatch does, spending 30 inside the transaction.
	tx := db.Begin()
	if err := tx.Exec(`SELECT balance FROM identities WHERE id = 1 FOR UPDATE`).Error; err != nil {
		t.Fatalf("lock identity: %v", err)
	}
	if err := tx.Exec(`UPDATE identities SET balance = balance - 30 WHERE id = 1`).Error; err != nil {
		t.Fatalf("spend balance: %v", err)
	}

	type result struct {
		res increaseBalanceResponse
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, err := increaseBalance(context.Background(), db, 1, "k1", 50)
		done <- result{res, err}
	}()

	select {
	case <-done:
		t.Fatal("increaseBalance() returned while the identity row was still locked")
	case <-time.After(100 * time.Millisecond):
	}

	if err := tx.Commit().Error; err != nil {
		t.Fatalf("commit: %v", err)
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("increaseBalance() error = %v", got.err)
	}
	if got.res.Balance != 120 {
		t.Errorf("increaseBalance() balance = %d, want 120 (built on the committed spend)", got.res.Balance)
	}
	if b := dbtest.IdentityBalance(t, db, 1); b != 120 {
		t.Errorf("identity balance = %d, want 120", b)
	}
}
