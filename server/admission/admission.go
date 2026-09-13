// Package admission batches incoming send requests, and forwards what's affordable to dispatch.
package admission

import (
	"context"
	"log/slog"
	"maps"
	"time"

	"gorm.io/gorm"

	"pigeon/server/dispatch"
)

type Config struct {
	Window  time.Duration
	MaxSize int
	Price   int64
}

// Admitter batches incoming requests, locks balance against them in Postgres, and forwards what's affordable to
// dispatch. It runs single-threaded so the balance arithmetic stays race-free without per-identity locking.
type Admitter struct {
	in      chan *Request
	window  time.Duration
	maxSize int

	db    *gorm.DB
	price int64

	expressCh chan<- dispatch.Item
	regularCh chan<- dispatch.Item
}

func New(cfg Config, db *gorm.DB, expressCh, regularCh chan<- dispatch.Item) *Admitter {
	return &Admitter{
		in:        make(chan *Request, cfg.MaxSize*4),
		window:    cfg.Window,
		maxSize:   cfg.MaxSize,
		db:        db,
		price:     cfg.Price,
		expressCh: expressCh,
		regularCh: regularCh,
	}
}

// Submit enqueues req and blocks if the admission channel is full.
func (b *Admitter) Submit(req *Request) {
	b.in <- req
}

func (b *Admitter) Run(ctx context.Context) {
	ticker := time.NewTicker(b.window)
	defer ticker.Stop()

	var pending []*Request

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
			// ctx is already cancelled here, so process any pending requests against a fresh context. The old one
			// would make their balance-lock transaction fail immediately.
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

func (b *Admitter) processBatch(ctx context.Context, pending []*Request) {
	order := make([]int64, 0)
	byIdentity := make(map[int64][]Message)

	for _, req := range pending {
		if _, seen := byIdentity[req.IdentityID]; !seen {
			order = append(order, req.IdentityID)
		}
		byIdentity[req.IdentityID] = append(byIdentity[req.IdentityID], req.Messages...)
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
			req.ResultCh <- Result{
				Status:   StatusErrTotalBalanceHit,
				Rejected: len(req.Messages),
				Total:    len(req.Messages),
			}
		}
		return
	}

	remaining := make(map[int64]int, len(accepted))
	maps.Copy(remaining, accepted)

	receivedAt := time.Now()

	for _, req := range pending {
		avail := remaining[req.IdentityID]
		admit := min(avail, len(req.Messages))
		remaining[req.IdentityID] = avail - admit

		for i := range admit {
			m := req.Messages[i]
			item := dispatch.Item{
				IdentityID: req.IdentityID,
				HourBucket: hourBucket,
				To:         m.To,
				Text:       m.Text,
				Cost:       b.price,
				ReceivedAt: receivedAt,
			}
			if req.Express {
				b.expressCh <- item
			} else {
				b.regularCh <- item
			}
		}

		var status string
		switch {
		case admit == len(req.Messages):
			status = StatusOK
		case admit == 0:
			status = StatusErrTotalBalanceHit
		default:
			status = StatusErrPartiallySentBalanceHit
		}

		req.ResultCh <- Result{
			Status:   status,
			Accepted: admit,
			Rejected: len(req.Messages) - admit,
			Total:    len(req.Messages),
		}
	}
}

// identityAdmission is one identity's messages within a single admission batch, in arrival order.
type identityAdmission struct {
	identityID int64
	messages   []Message
}

// lockBatch runs one transaction per admission batch. For each identity it locks the balance row, admits as many
// messages as the balance covers (in arrival order), deducts the spend, and folds it into the identity's current-hour
// balance_locks row. Returns how many of each identity's messages were admitted.
func (b *Admitter) lockBatch(
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
