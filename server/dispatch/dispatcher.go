// Package dispatch batches admitted messages and hands them to the operator.
package dispatch

import (
	"context"
	"log/slog"
	"time"

	"gorm.io/gorm"
)

// Item is one accepted, paid message waiting to be sent to the operator. HourBucket + Cost identify which balance_locks
// row to release once the operator confirms delivery.
type Item struct {
	IdentityID int64
	HourBucket time.Time
	To         string
	Text       string
	Cost       int64
	ReceivedAt time.Time
}

// Operator is what the dispatcher sends confirmed-paid batches to.
type Operator interface {
	Send(ctx context.Context, batch []Item) error
}

type Config struct {
	Window          time.Duration
	MaxSize         int
	Expiry          time.Duration
	OperatorTimeout time.Duration
}

// Dispatcher batches accepted messages from one channel (express or regular) and hands them to the operator. Each
// flushed batch is passed to its own goroutine so one delivery attempt never blocks new batches from forming.
//
// There is deliberately no retry on failure due to the SLA between us and the operator and the possibility of us either
// breaking the contract by sending more rps than declared or violating our own system availability.
type Dispatcher struct {
	name    string
	in      <-chan Item
	window  time.Duration
	maxSize int

	expiry          time.Duration
	operatorTimeout time.Duration

	operator Operator
	db       *gorm.DB
}

func New(name string, cfg Config, in <-chan Item, db *gorm.DB, operator Operator) *Dispatcher {
	return &Dispatcher{
		name: name, in: in, window: cfg.Window, maxSize: cfg.MaxSize,
		expiry: cfg.Expiry, operatorTimeout: cfg.OperatorTimeout,
		operator: operator, db: db,
	}
}

func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(d.window)
	defer ticker.Stop()

	var pending []Item

	for {
		select {
		case <-ctx.Done():
			if len(pending) > 0 {
				d.deliver(pending)
			}
			return
		case item := <-d.in:
			pending = append(pending, item)
			if len(pending) >= d.maxSize {
				go d.deliver(pending) //nolint:gosec
				pending = nil
			}
		case <-ticker.C:
			if len(pending) > 0 {
				go d.deliver(pending) //nolint:gosec
				pending = nil
			}
		}
	}
}

// deliver makes exactly one attempt to hand batch to the operator, limited to `operatorTimeout`. On any failure,
// the batch is dropped.
func (d *Dispatcher) deliver(batch []Item) {
	batch = d.dropExpired(batch)
	if len(batch) == 0 {
		return
	}

	sendCtx, cancel := context.WithTimeout(context.Background(), d.operatorTimeout)
	defer cancel()

	if err := d.operator.Send(sendCtx, batch); err != nil {
		slog.Error("dispatch: operator send failed, dropping batch", "dispatcher", d.name, "error", err)
		return
	}

	finalizeCtx, cancel2 := context.WithTimeout(context.Background(), d.operatorTimeout)
	defer cancel2()

	if err := d.finalize(finalizeCtx, batch); err != nil {
		slog.Error("dispatch: finalize failed", "dispatcher", d.name, "error", err)
	}
}

func (d *Dispatcher) dropExpired(batch []Item) []Item {
	now := time.Now()
	kept := batch[:0]
	for _, item := range batch {
		if now.Sub(item.ReceivedAt) <= d.expiry {
			kept = append(kept, item)
		}
	}
	return kept
}

// finalize releases the balance_locks amount and records report_stats for every (identity, hour bucket) represented
// in batch, since the operator has confirmed it.
func (d *Dispatcher) finalize(ctx context.Context, batch []Item) error {
	type key struct {
		identityID int64
		hourBucket time.Time
	}
	type stats struct {
		sent   int64
		amount int64
	}

	byKey := make(map[key]stats)
	for _, item := range batch {
		k := key{item.IdentityID, item.HourBucket}
		s := byKey[k]
		s.sent++
		s.amount += item.Cost
		byKey[k] = s
	}

	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for k, s := range byKey {
			if err := tx.Exec(`
				UPDATE balance_locks SET amount = GREATEST(amount - ?, 0)
				WHERE identity_id = ? AND hour_bucket = ?
			`, s.amount, k.identityID, k.hourBucket).Error; err != nil {
				return err
			}

			if err := tx.Exec(`
				INSERT INTO report_stats (identity_id, hour_bucket, messages_sent, amount_spent)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (identity_id, hour_bucket)
				DO UPDATE SET
					messages_sent = report_stats.messages_sent + EXCLUDED.messages_sent,
					amount_spent = report_stats.amount_spent + EXCLUDED.amount_spent
			`, k.identityID, k.hourBucket, s.sent, s.amount).Error; err != nil {
				return err
			}
		}
		return nil
	})
}
