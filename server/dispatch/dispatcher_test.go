package dispatch

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"pigeon/internal/dbtest"
)

func TestMain(m *testing.M) {
	os.Exit(dbtest.Main(m))
}

var errSend = errors.New("send failed")

// fakeOperator records every batch it receives and can be made to fail deterministically.
type fakeOperator struct {
	mu      sync.Mutex
	calls   [][]Item
	sendErr error
}

func (s *fakeOperator) Send(_ context.Context, batch []Item) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make([]Item, len(batch))
	copy(cp, batch)
	s.calls = append(s.calls, cp)

	return s.sendErr
}

func (s *fakeOperator) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *fakeOperator) lastBatch() []Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		return nil
	}
	return s.calls[len(s.calls)-1]
}

// waitFor polls cond until it returns true or timeout happens, failing the test otherwise. Used for assertions against
// work done in a background goroutine (e.g. an async dispatch flush).
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

func TestDispatcher_DropExpired(t *testing.T) {
	now := time.Now()
	d := &Dispatcher{expiry: time.Minute}

	batch := []Item{
		{IdentityID: 1, ReceivedAt: now},
		{IdentityID: 2, ReceivedAt: now.Add(-2 * time.Minute)},
		{IdentityID: 3, ReceivedAt: now.Add(-30 * time.Second)},
	}

	kept := d.dropExpired(batch)

	if len(kept) != 2 {
		t.Fatalf("dropExpired() kept %d items, want 2: %+v", len(kept), kept)
	}
	for _, item := range kept {
		if item.IdentityID == 2 {
			t.Fatalf("dropExpired() kept expired item %+v", item)
		}
	}
}

func TestDispatcher_Deliver_AllExpiredSkipsOperator(t *testing.T) {
	op := &fakeOperator{}
	d := &Dispatcher{expiry: time.Minute, operatorTimeout: time.Second, operator: op}

	d.deliver([]Item{{ReceivedAt: time.Now().Add(-time.Hour)}})

	if op.callCount() != 0 {
		t.Fatalf("operator.Send called %d times, want 0", op.callCount())
	}
}

func TestDispatcher_Deliver_OperatorFailureRefundsBalanceLocks(t *testing.T) {
	db := dbtest.Open(t)

	hourBucket := time.Now().UTC().Truncate(time.Hour)
	dbtest.SeedIdentity(t, db, 1, 100)
	dbtest.SeedBalanceLock(t, db, 1, hourBucket, 40)
	dbtest.SeedIdentity(t, db, 2, 0)
	dbtest.SeedBalanceLock(t, db, 2, hourBucket, 5)

	op := &fakeOperator{sendErr: errSend}
	d := &Dispatcher{name: "test", expiry: time.Hour, operatorTimeout: time.Second, operator: op, db: db}

	batch := []Item{
		{IdentityID: 1, HourBucket: hourBucket, Cost: 15, ReceivedAt: time.Now()},
		{IdentityID: 2, HourBucket: hourBucket, Cost: 5, ReceivedAt: time.Now()},
		{IdentityID: 1, HourBucket: hourBucket, Cost: 10, ReceivedAt: time.Now()},
	}
	d.deliver(batch)

	if op.callCount() != 1 {
		t.Fatalf("operator.Send called %d times, want 1", op.callCount())
	}

	if got, want := dbtest.BalanceLockAmount(t, db, 1, hourBucket), int64(15); got != want {
		t.Fatalf("identity 1 balance_locks.amount = %d, want %d", got, want)
	}
	if got, want := dbtest.IdentityBalance(t, db, 1), int64(125); got != want {
		t.Fatalf("identity 1 balance = %d, want %d", got, want)
	}
	if got, want := dbtest.BalanceLockAmount(t, db, 2, hourBucket), int64(0); got != want {
		t.Fatalf("identity 2 balance_locks.amount = %d, want %d", got, want)
	}
	if got, want := dbtest.IdentityBalance(t, db, 2), int64(5); got != want {
		t.Fatalf("identity 2 balance = %d, want %d", got, want)
	}

	var reports int64
	if err := db.Raw(`SELECT count(*) FROM report_stats`).Scan(&reports).Error; err != nil {
		t.Fatalf("count report_stats: %v", err)
	}
	if reports != 0 {
		t.Fatalf("report_stats has %d rows after a failed send, want 0", reports)
	}
}

func TestDispatcher_Deliver_OperatorFailureRefundCappedAtLockedAmount(t *testing.T) {
	db := dbtest.Open(t)

	hourBucket := time.Now().UTC().Truncate(time.Hour)
	dbtest.SeedIdentity(t, db, 1, 100)
	dbtest.SeedBalanceLock(t, db, 1, hourBucket, 10)
	// identity 2's lock row was already refunded and cleared by the reaper.
	dbtest.SeedIdentity(t, db, 2, 50)

	op := &fakeOperator{sendErr: errSend}
	d := &Dispatcher{name: "test", expiry: time.Hour, operatorTimeout: time.Second, operator: op, db: db}

	batch := []Item{
		{IdentityID: 1, HourBucket: hourBucket, Cost: 25, ReceivedAt: time.Now()},
		{IdentityID: 2, HourBucket: hourBucket, Cost: 25, ReceivedAt: time.Now()},
	}
	d.deliver(batch)

	if got, want := dbtest.BalanceLockAmount(t, db, 1, hourBucket), int64(0); got != want {
		t.Fatalf("identity 1 balance_locks.amount = %d, want %d", got, want)
	}
	if got, want := dbtest.IdentityBalance(t, db, 1), int64(110); got != want {
		t.Fatalf("identity 1 balance = %d, want %d", got, want)
	}
	if dbtest.BalanceLockExists(t, db, 2, hourBucket) {
		t.Fatal("refund recreated identity 2's cleared balance_locks row")
	}
	if got, want := dbtest.IdentityBalance(t, db, 2), int64(50); got != want {
		t.Fatalf("identity 2 balance = %d, want %d", got, want)
	}
}

func TestDispatcher_Deliver_SuccessFinalizesBalanceLocks(t *testing.T) {
	db := dbtest.Open(t)

	hourBucket := time.Now().UTC().Truncate(time.Hour)
	dbtest.SeedIdentity(t, db, 1, 100)
	dbtest.SeedBalanceLock(t, db, 1, hourBucket, 40)

	op := &fakeOperator{}
	d := &Dispatcher{name: "test", expiry: time.Hour, operatorTimeout: time.Second, operator: op, db: db}

	batch := []Item{
		{IdentityID: 1, HourBucket: hourBucket, Cost: 15, ReceivedAt: time.Now()},
		{IdentityID: 1, HourBucket: hourBucket, Cost: 10, ReceivedAt: time.Now()},
	}
	d.deliver(batch)

	if op.callCount() != 1 {
		t.Fatalf("operator.Send called %d times, want 1", op.callCount())
	}

	if got, want := dbtest.BalanceLockAmount(t, db, 1, hourBucket), int64(15); got != want {
		t.Fatalf("balance_locks.amount = %d, want %d", got, want)
	}

	sent, spent := dbtest.ReportStats(t, db, 1, hourBucket)
	if sent != 2 || spent != 25 {
		t.Fatalf("report_stats = (sent=%d, spent=%d), want (sent=2, spent=25)", sent, spent)
	}
}

func TestDispatcher_Run_BatchesUpToMaxSize(t *testing.T) {
	op := &fakeOperator{sendErr: errSend} // failing operator: never reaches finalize/db.
	in := make(chan Item, 10)
	d := &Dispatcher{
		name: "test", in: in, window: time.Hour, maxSize: 3,
		expiry: time.Hour, operatorTimeout: time.Second, operator: op,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	for i := range 3 {
		in <- Item{IdentityID: int64(i), ReceivedAt: time.Now()}
	}

	waitFor(t, func() bool { return op.callCount() == 1 }, time.Second)
	if got := len(op.lastBatch()); got != 3 {
		t.Fatalf("dispatched batch size = %d, want 3", got)
	}

	cancel()
	<-done
}

func TestDispatcher_Run_FlushesOnWindowTicker(t *testing.T) {
	op := &fakeOperator{sendErr: errSend}
	in := make(chan Item, 10)
	d := &Dispatcher{
		name: "test", in: in, window: 10 * time.Millisecond, maxSize: 100,
		expiry: time.Hour, operatorTimeout: time.Second, operator: op,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	in <- Item{IdentityID: 1, ReceivedAt: time.Now()}

	waitFor(t, func() bool { return op.callCount() == 1 }, time.Second)
	if got := len(op.lastBatch()); got != 1 {
		t.Fatalf("dispatched batch size = %d, want 1", got)
	}

	cancel()
	<-done
}

func TestDispatcher_Run_FlushesPendingOnContextCancel(t *testing.T) {
	op := &fakeOperator{sendErr: errSend}
	in := make(chan Item, 10)
	d := &Dispatcher{
		name: "test", in: in, window: time.Hour, maxSize: 100,
		expiry: time.Hour, operatorTimeout: time.Second, operator: op,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()

	in <- Item{IdentityID: 1, ReceivedAt: time.Now()}
	time.Sleep(20 * time.Millisecond) // let it land in `pending` before cancelling.
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after ctx cancellation")
	}

	if op.callCount() != 1 {
		t.Fatalf("operator.Send called %d times after shutdown, want 1", op.callCount())
	}
}
