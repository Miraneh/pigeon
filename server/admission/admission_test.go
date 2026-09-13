package admission

import (
	"context"
	"os"
	"testing"
	"time"

	"gorm.io/gorm"

	"pigeon/internal/dbtest"
	"pigeon/server/dispatch"
)

func TestMain(m *testing.M) {
	os.Exit(dbtest.Main(m))
}

// newAdmitterForTest builds an admitter with real channels and a fixed price of 1.
func newAdmitterForTest(db *gorm.DB, window time.Duration, maxSize int) *Admitter {
	return &Admitter{
		in:        make(chan *Request, maxSize*4+10),
		window:    window,
		maxSize:   maxSize,
		db:        db,
		price:     1,
		expressCh: make(chan dispatch.Item, 100),
		regularCh: make(chan dispatch.Item, 100),
	}
}

func TestAdmitter_LockBatch(t *testing.T) {
	db := dbtest.Open(t)
	b := &Admitter{db: db, price: 10}

	dbtest.SeedIdentity(t, db, 1, 25) // covers 2 messages (20), 5 left over
	dbtest.SeedIdentity(t, db, 2, 0)  // covers nothing

	hourBucket := time.Now().UTC().Truncate(time.Hour)
	admissions := []identityAdmission{
		{identityID: 1, messages: []Message{{To: "a"}, {To: "b"}, {To: "c"}}},
		{identityID: 2, messages: []Message{{To: "d"}}},
	}

	accepted, err := b.lockBatch(context.Background(), admissions, hourBucket)
	if err != nil {
		t.Fatalf("lockBatch() error = %v", err)
	}

	if accepted[1] != 2 {
		t.Errorf("accepted[1] = %d, want 2", accepted[1])
	}
	if accepted[2] != 0 {
		t.Errorf("accepted[2] = %d, want 0", accepted[2])
	}
	if got := dbtest.IdentityBalance(t, db, 1); got != 5 {
		t.Errorf("identity 1 balance = %d, want 5", got)
	}
	if got := dbtest.BalanceLockAmount(t, db, 1, hourBucket); got != 20 {
		t.Errorf("balance_locks amount for identity 1 = %d, want 20", got)
	}
	if got := dbtest.IdentityBalance(t, db, 2); got != 0 {
		t.Errorf("identity 2 balance = %d, want 0", got)
	}
}

func TestAdmitter_LockBatch_AccumulatesAcrossCalls(t *testing.T) {
	db := dbtest.Open(t)
	b := &Admitter{db: db, price: 5}
	dbtest.SeedIdentity(t, db, 1, 100)

	hourBucket := time.Now().UTC().Truncate(time.Hour)
	admissions := []identityAdmission{{identityID: 1, messages: []Message{{To: "a"}}}}

	if _, err := b.lockBatch(context.Background(), admissions, hourBucket); err != nil {
		t.Fatalf("first lockBatch() error = %v", err)
	}
	if _, err := b.lockBatch(context.Background(), admissions, hourBucket); err != nil {
		t.Fatalf("second lockBatch() error = %v", err)
	}

	if got := dbtest.BalanceLockAmount(t, db, 1, hourBucket); got != 10 {
		t.Errorf("balance_locks amount = %d, want 10 (accumulated across two calls)", got)
	}
	if got := dbtest.IdentityBalance(t, db, 1); got != 90 {
		t.Errorf("identity balance = %d, want 90", got)
	}
}

func TestAdmitter_LockBatch_CreatesUnknownIdentityAtZeroBalance(t *testing.T) {
	db := dbtest.Open(t)
	b := &Admitter{db: db, price: 1}

	hourBucket := time.Now().UTC().Truncate(time.Hour)
	admissions := []identityAdmission{{identityID: 999, messages: []Message{{To: "a"}}}}

	accepted, err := b.lockBatch(context.Background(), admissions, hourBucket)
	if err != nil {
		t.Fatalf("lockBatch() error = %v", err)
	}
	if accepted[999] != 0 {
		t.Errorf("accepted[999] = %d, want 0", accepted[999])
	}
	if got := dbtest.IdentityBalance(t, db, 999); got != 0 {
		t.Errorf("identity 999 balance = %d, want 0", got)
	}
}

func TestAdmitter_ProcessBatch_RoutesToExpressAndRegularChannels(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 100)

	expressCh := make(chan dispatch.Item, 10)
	regularCh := make(chan dispatch.Item, 10)
	b := &Admitter{db: db, price: 1, expressCh: expressCh, regularCh: regularCh}

	expressReq := &Request{
		IdentityID: 1, Express: true,
		Messages: []Message{{To: "a"}}, ResultCh: make(chan Result, 1),
	}
	regularReq := &Request{
		IdentityID: 1, Express: false,
		Messages: []Message{{To: "b"}}, ResultCh: make(chan Result, 1),
	}

	b.processBatch(context.Background(), []*Request{expressReq, regularReq})

	select {
	case item := <-expressCh:
		if item.To != "a" {
			t.Errorf("expressCh got %+v, want message to \"a\"", item)
		}
	default:
		t.Fatal("expected an item on expressCh")
	}
	select {
	case item := <-regularCh:
		if item.To != "b" {
			t.Errorf("regularCh got %+v, want message to \"b\"", item)
		}
	default:
		t.Fatal("expected an item on regularCh")
	}

	res := <-expressReq.ResultCh
	if res.Status != StatusOK || res.Accepted != 1 {
		t.Errorf("expressReq result = %+v, want status ok / accepted 1", res)
	}
}

func TestAdmitter_ProcessBatch_PartialAndTotalBalanceHit(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 1) // only 1 of 2 messages affordable

	b := &Admitter{
		db: db, price: 1,
		expressCh: make(chan dispatch.Item, 10), regularCh: make(chan dispatch.Item, 10),
	}

	req := &Request{
		IdentityID: 1,
		Messages:   []Message{{To: "a"}, {To: "b"}},
		ResultCh:   make(chan Result, 1),
	}

	b.processBatch(context.Background(), []*Request{req})

	res := <-req.ResultCh
	if res.Status != StatusErrPartiallySentBalanceHit {
		t.Errorf("Status = %q, want %q", res.Status, StatusErrPartiallySentBalanceHit)
	}
	if res.Accepted != 1 || res.Rejected != 1 || res.Total != 2 {
		t.Errorf("result = %+v, want Accepted=1 Rejected=1 Total=2", res)
	}
}

func TestAdmitter_ProcessBatch_LockBatchFailureRejectsAll(t *testing.T) {
	db := dbtest.Open(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get underlying sql.DB: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	b := &Admitter{
		db: db, price: 1, expressCh: make(chan dispatch.Item, 1), regularCh: make(chan dispatch.Item, 1),
	}
	req := &Request{IdentityID: 1, Messages: []Message{{To: "a"}}, ResultCh: make(chan Result, 1)}

	b.processBatch(context.Background(), []*Request{req})

	res := <-req.ResultCh
	if res.Status != StatusErrTotalBalanceHit || res.Rejected != 1 || res.Total != 1 {
		t.Errorf("result = %+v, want a total-balance-hit rejection", res)
	}
}

func TestAdmitter_Run_FlushesPendingOnContextCancel(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 100)

	b := newAdmitterForTest(db, time.Hour, 100)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		b.Run(ctx)
		close(done)
	}()

	req := &Request{IdentityID: 1, Messages: []Message{{To: "a"}}, ResultCh: make(chan Result, 1)}
	b.Submit(req)
	time.Sleep(20 * time.Millisecond) // let it land in `pending` before cancelling
	cancel()

	select {
	case res := <-req.ResultCh:
		if res.Status != StatusOK {
			t.Errorf("result status = %q, want ok", res.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not flush pending request on ctx cancellation")
	}
	<-done
}

func TestAdmitter_Run_FlushesAtMaxSize(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 1, 100)

	b := newAdmitterForTest(db, time.Hour, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		b.Run(ctx)
		close(done)
	}()

	req := &Request{IdentityID: 1, Messages: []Message{{To: "a"}}, ResultCh: make(chan Result, 1)}
	b.Submit(req)

	select {
	case res := <-req.ResultCh:
		if res.Status != StatusOK {
			t.Errorf("result status = %q, want ok", res.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not flush at maxSize=1")
	}

	cancel()
	<-done
}
