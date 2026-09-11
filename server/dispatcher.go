package main

import (
	"context"
	"flag"
	"log/slog"
	"time"

	"gorm.io/gorm"
)

var (
	expressWindowFlag  = flag.Duration("express-window", 500*time.Millisecond, "express dispatch flush interval")
	expressMaxSizeFlag = flag.Int("express-max-size", 300, "express dispatch max batch size")
	regularWindowFlag  = flag.Duration("regular-window", time.Second, "regular dispatch flush interval")
	regularMaxSizeFlag = flag.Int("regular-max-size", 1000, "regular dispatch max batch size")

	expiryFlag = flag.Duration(
		"expiry", 3*time.Hour, "how long a message may wait before being dropped undelivered",
	)
	operatorTimeoutFlag = flag.Duration(
		"operator-timeout", 500*time.Millisecond, "bound on one operator call, well above its own p99.9",
	)
)

// dispatcher batches accepted messages from one channel (express or regular) and hands them to the operator. Each
// flushed batch is passed to its own goroutine so one delivery attempt never blocks new batches from forming.
//
// There is deliberately no retry on failure due to the SLA between us and the operator and the possibility of us either
// breaking the contract by sending more rps than declared or violating our own system availability.
type dispatcher struct {
	name    string
	in      <-chan dispatchItem
	window  time.Duration
	maxSize int

	expiry          time.Duration
	operatorTimeout time.Duration

	operator operatorClient
	db       *gorm.DB
}

func newDispatcher(
	name string, in <-chan dispatchItem, window time.Duration, maxSize int, db *gorm.DB, operator operatorClient,
) *dispatcher {
	return &dispatcher{
		name: name, in: in, window: window, maxSize: maxSize,
		expiry: *expiryFlag, operatorTimeout: *operatorTimeoutFlag,
		operator: operator, db: db,
	}
}

func (d *dispatcher) run(ctx context.Context) {
	ticker := time.NewTicker(d.window)
	defer ticker.Stop()

	var pending []dispatchItem

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
func (d *dispatcher) deliver(batch []dispatchItem) {
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

func (d *dispatcher) dropExpired(batch []dispatchItem) []dispatchItem {
	now := time.Now()
	kept := batch[:0]
	for _, item := range batch {
		if now.Sub(item.receivedAt) <= d.expiry {
			kept = append(kept, item)
		}
	}
	return kept
}

// finalize releases the balance_locks amount and records report_stats for every (identity, hour bucket) represented
// in batch, since the operator has confirmed it. This is not an update on the identity's balance as that has already
// been handled.
func (d *dispatcher) finalize(ctx context.Context, batch []dispatchItem) error {
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
		k := key{item.identityID, item.hourBucket}
		s := byKey[k]
		s.sent++
		s.amount += item.cost
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
