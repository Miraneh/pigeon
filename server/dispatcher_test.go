package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// spyOperator records every batch it receives and can be made to fail deterministically.
type spyOperator struct {
	mu      sync.Mutex
	calls   [][]dispatchItem
	sendErr error
}

func (s *spyOperator) Send(_ context.Context, batch []dispatchItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make([]dispatchItem, len(batch))
	copy(cp, batch)
	s.calls = append(s.calls, cp)

	return s.sendErr
}

func (s *spyOperator) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *spyOperator) lastBatch() []dispatchItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		return nil
	}
	return s.calls[len(s.calls)-1]
}

func TestDispatcher_DropExpired(t *testing.T) {
	now := time.Now()
	d := &dispatcher{expiry: time.Minute}

	batch := []dispatchItem{
		{identityID: 1, receivedAt: now},
		{identityID: 2, receivedAt: now.Add(-2 * time.Minute)},
		{identityID: 3, receivedAt: now.Add(-30 * time.Second)},
	}

	kept := d.dropExpired(batch)

	if len(kept) != 2 {
		t.Fatalf("dropExpired() kept %d items, want 2: %+v", len(kept), kept)
	}
	for _, item := range kept {
		if item.identityID == 2 {
			t.Fatalf("dropExpired() kept expired item %+v", item)
		}
	}
}

func TestDispatcher_Deliver_AllExpiredSkipsOperator(t *testing.T) {
	op := &spyOperator{}
	d := &dispatcher{expiry: time.Minute, operatorTimeout: time.Second, operator: op}

	d.deliver([]dispatchItem{{receivedAt: time.Now().Add(-time.Hour)}})

	if op.callCount() != 0 {
		t.Fatalf("operator.Send called %d times, want 0", op.callCount())
	}
}

func TestDispatcher_Deliver_OperatorFailureSkipsFinalize(t *testing.T) {
	op := &spyOperator{sendErr: errOperatorUnavailable}
	d := &dispatcher{name: "test", expiry: time.Hour, operatorTimeout: time.Second, operator: op}

	// db is intentionally left nil: finalize must not be reached when Send fails, or this would panic.
	d.deliver([]dispatchItem{{receivedAt: time.Now()}})

	if op.callCount() != 1 {
		t.Fatalf("operator.Send called %d times, want 1", op.callCount())
	}
}

func TestDispatcher_Deliver_SuccessFinalizesBalanceLocks(t *testing.T) {
	db := openTestDB(t)

	hourBucket := time.Now().UTC().Truncate(time.Hour)
	seedIdentity(t, db, 1, 100)
	seedBalanceLock(t, db, 1, hourBucket, 40)

	op := &spyOperator{}
	d := &dispatcher{name: "test", expiry: time.Hour, operatorTimeout: time.Second, operator: op, db: db}

	batch := []dispatchItem{
		{identityID: 1, hourBucket: hourBucket, cost: 15, receivedAt: time.Now()},
		{identityID: 1, hourBucket: hourBucket, cost: 10, receivedAt: time.Now()},
	}
	d.deliver(batch)

	if op.callCount() != 1 {
		t.Fatalf("operator.Send called %d times, want 1", op.callCount())
	}

	if got, want := balanceLockAmount(t, db, 1, hourBucket), int64(15); got != want {
		t.Fatalf("balance_locks.amount = %d, want %d", got, want)
	}

	sent, spent := reportStatsOf(t, db, 1, hourBucket)
	if sent != 2 || spent != 25 {
		t.Fatalf("report_stats = (sent=%d, spent=%d), want (sent=2, spent=25)", sent, spent)
	}
}

func TestDispatcher_Run_BatchesUpToMaxSize(t *testing.T) {
	op := &spyOperator{sendErr: errOperatorUnavailable} // failing operator: never reaches finalize/db
	in := make(chan dispatchItem, 10)
	d := &dispatcher{
		name: "test", in: in, window: time.Hour, maxSize: 3,
		expiry: time.Hour, operatorTimeout: time.Second, operator: op,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.run(ctx)
		close(done)
	}()

	for i := range 3 {
		in <- dispatchItem{identityID: int64(i), receivedAt: time.Now()}
	}

	waitFor(t, func() bool { return op.callCount() == 1 }, time.Second)
	if got := len(op.lastBatch()); got != 3 {
		t.Fatalf("dispatched batch size = %d, want 3", got)
	}

	cancel()
	<-done
}

func TestDispatcher_Run_FlushesOnWindowTicker(t *testing.T) {
	op := &spyOperator{sendErr: errOperatorUnavailable}
	in := make(chan dispatchItem, 10)
	d := &dispatcher{
		name: "test", in: in, window: 10 * time.Millisecond, maxSize: 100,
		expiry: time.Hour, operatorTimeout: time.Second, operator: op,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.run(ctx)
		close(done)
	}()

	in <- dispatchItem{identityID: 1, receivedAt: time.Now()}

	waitFor(t, func() bool { return op.callCount() == 1 }, time.Second)
	if got := len(op.lastBatch()); got != 1 {
		t.Fatalf("dispatched batch size = %d, want 1", got)
	}

	cancel()
	<-done
}

func TestDispatcher_Run_FlushesPendingOnContextCancel(t *testing.T) {
	op := &spyOperator{sendErr: errOperatorUnavailable}
	in := make(chan dispatchItem, 10)
	d := &dispatcher{
		name: "test", in: in, window: time.Hour, maxSize: 100,
		expiry: time.Hour, operatorTimeout: time.Second, operator: op,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		d.run(ctx)
		close(done)
	}()

	in <- dispatchItem{identityID: 1, receivedAt: time.Now()}
	time.Sleep(20 * time.Millisecond) // let it land in `pending` before cancelling
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run() did not return after ctx cancellation")
	}

	if op.callCount() != 1 {
		t.Fatalf("operator.Send called %d times after shutdown, want 1", op.callCount())
	}
}
