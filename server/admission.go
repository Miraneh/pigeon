package main

import (
	"context"
	"flag"
	"log/slog"
	"maps"
	"time"

	"gorm.io/gorm"
)

var (
	admissionWindowFlag  = flag.Duration("admission-window", 50*time.Millisecond, "admission batch flush interval")
	admissionMaxSizeFlag = flag.Int("admission-max-size", 5000, "admission batch max size before an early flush")
	pricePerSMSFlag      = flag.Int64("price-per-sms", 1, "cost of one SMS, in balance units")
)

// admitter batches incoming requests, locks balance against them in Postgres, and forwards what's affordable to
// dispatch. It runs single-threaded so the balance arithmetic stays race-free without per-identity locking.
type admitter struct {
	in      chan *admissionRequest
	window  time.Duration
	maxSize int

	db    *gorm.DB
	price int64

	expressCh chan<- dispatchItem
	regularCh chan<- dispatchItem
}

func newAdmitter(db *gorm.DB, expressCh, regularCh chan<- dispatchItem) *admitter {
	maxSize := *admissionMaxSizeFlag
	return &admitter{
		in:        make(chan *admissionRequest, maxSize*4),
		window:    *admissionWindowFlag,
		maxSize:   maxSize,
		db:        db,
		price:     *pricePerSMSFlag,
		expressCh: expressCh,
		regularCh: regularCh,
	}
}

// submit enqueues req and blocks if the admission channel is full.
func (b *admitter) submit(req *admissionRequest) {
	b.in <- req
}

func (b *admitter) run(ctx context.Context) {
	ticker := time.NewTicker(b.window)
	defer ticker.Stop()

	var pending []*admissionRequest

	flush := func(flushCtx context.Context) {
		if len(pending) == 0 {
			return
		}
		b.processBatch(flushCtx, pending)
		pending = nil
	}

	for {
		select {
		case <-ctx.Done():
			// ctx is already cancelled here, so process any pending requests against a fresh context instead
			// of one that would make their balance-lock transaction fail immediately.
			flush(context.Background())
			return
		case req := <-b.in:
			pending = append(pending, req)
			if len(pending) >= b.maxSize {
				flush(ctx)
			}
		case <-ticker.C:
			flush(ctx)
		}
	}
}

func (b *admitter) processBatch(ctx context.Context, pending []*admissionRequest) {
	order := make([]int64, 0)
	byIdentity := make(map[int64][]admissionMsg)

	for _, req := range pending {
		if _, seen := byIdentity[req.identityID]; !seen {
			order = append(order, req.identityID)
		}
		byIdentity[req.identityID] = append(byIdentity[req.identityID], req.messages...)
	}

	admissions := make([]identityAdmission, 0, len(order))
	for _, id := range order {
		admissions = append(admissions, identityAdmission{identityID: id, messages: byIdentity[id]})
	}

	hourBucket := time.Now().UTC().Truncate(time.Hour)

	accepted, err := b.lockBatch(ctx, admissions, hourBucket)
	if err != nil {
		slog.Error("admission: lock batch failed", "error", err)
		for _, req := range pending {
			req.resultCh <- sendResult{
				Status:   statusErrTotalBalanceHit,
				Rejected: len(req.messages),
				Total:    len(req.messages),
			}
		}
		return
	}

	remaining := make(map[int64]int, len(accepted))
	maps.Copy(remaining, accepted)

	receivedAt := time.Now()

	for _, req := range pending {
		avail := remaining[req.identityID]
		admit := min(avail, len(req.messages))
		remaining[req.identityID] = avail - admit

		for i := range admit {
			m := req.messages[i]
			item := dispatchItem{
				identityID: req.identityID,
				hourBucket: hourBucket,
				to:         m.to,
				text:       m.text,
				cost:       b.price,
				receivedAt: receivedAt,
			}
			if req.express {
				b.expressCh <- item
			} else {
				b.regularCh <- item
			}
		}

		var status string
		switch {
		case admit == len(req.messages):
			status = statusOK
		case admit == 0:
			status = statusErrTotalBalanceHit
		default:
			status = statusErrPartiallySentBalanceHit
		}

		req.resultCh <- sendResult{
			Status:   status,
			Accepted: admit,
			Rejected: len(req.messages) - admit,
			Total:    len(req.messages),
		}
	}
}

// identityAdmission is one identity's messages within a single admission batch, in arrival order.
type identityAdmission struct {
	identityID int64
	messages   []admissionMsg
}

// lockBatch runs one transaction per admission batch. For each identity it locks the balance row, admits as many
// messages as the balance covers (in arrival order), deducts the spend, and folds it into the identity's current-hour
// balance_locks row. It returns how many of each identity's messages were admitted.
func (b *admitter) lockBatch(
	ctx context.Context,
	admissions []identityAdmission,
	hourBucket time.Time,
) (map[int64]int, error) {
	accepted := make(map[int64]int, len(admissions))

	err := b.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, a := range admissions {
			if err := tx.Exec(
				`INSERT INTO identities (id, balance) VALUES (?, 0) ON CONFLICT (id) DO NOTHING`,
				a.identityID,
			).Error; err != nil {
				return err
			}

			var balance int64
			if err := tx.Raw(
				`SELECT balance FROM identities WHERE id = ? FOR UPDATE`,
				a.identityID,
			).Scan(&balance).Error; err != nil {
				return err
			}

			total := int64(len(a.messages)) * b.price

			var admit int
			var spend int64
			switch {
			case balance >= total:
				admit, spend = len(a.messages), total
			case balance <= 0:
				admit, spend = 0, 0
			default:
				admit = int(balance / b.price)
				spend = int64(admit) * b.price
			}

			if spend > 0 {
				if err := tx.Exec(
					`UPDATE identities SET balance = balance - ? WHERE id = ?`,
					spend, a.identityID,
				).Error; err != nil {
					return err
				}

				if err := tx.Exec(`
					INSERT INTO balance_locks (identity_id, hour_bucket, amount)
					VALUES (?, ?, ?)
					ON CONFLICT (identity_id, hour_bucket)
					DO UPDATE SET amount = balance_locks.amount + EXCLUDED.amount
				`, a.identityID, hourBucket, spend).Error; err != nil {
					return err
				}
			}

			accepted[a.identityID] = admit
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return accepted, nil
}
