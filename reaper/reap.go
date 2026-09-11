package main

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// staleLock is one (identity, hour_bucket) balance_locks row past the expiry window.
type staleLock struct {
	IdentityID int64
	HourBucket time.Time
}

// reap finds every balance_locks row whose hour_bucket is older than expiry, refunds any amount still held on it
// back onto the identity's balance, and deletes the row, one transaction per row. It returns how many rows had
// a nonzero amount refunded.
func reap(ctx context.Context, db *gorm.DB, expiry time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-expiry)

	var staleLocks []staleLock
	err := db.WithContext(ctx).Raw(
		`SELECT identity_id, hour_bucket FROM balance_locks WHERE hour_bucket < ?`, cutoff,
	).Scan(&staleLocks).Error
	if err != nil {
		return 0, err
	}

	refunded := 0
	for _, s := range staleLocks {
		ok, err := refundLock(ctx, db, s.IdentityID, s.HourBucket)
		if err != nil {
			return refunded, err
		}
		if ok {
			refunded++
		}
	}

	return refunded, nil
}

// refundLock locks a single balance_locks row, credits whatever amount remains back onto the identity, and
// deletes the row, all in one transaction. amount is re-read under the lock since the dispatcher's finalize step
// can still be decrementing it concurrently right up until the moment it's deleted here.
func refundLock(ctx context.Context, db *gorm.DB, identityID int64, hourBucket time.Time) (bool, error) {
	refunded := false

	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var amount int64
		err := tx.Raw(
			`SELECT amount FROM balance_locks WHERE identity_id = ? AND hour_bucket = ? FOR UPDATE`,
			identityID, hourBucket,
		).Scan(&amount).Error
		if err != nil {
			return err
		}

		if amount > 0 {
			if err := tx.Exec(
				`UPDATE identities SET balance = balance + ? WHERE id = ?`, amount, identityID,
			).Error; err != nil {
				return err
			}
			refunded = true
		}

		return tx.Exec(
			`DELETE FROM balance_locks WHERE identity_id = ? AND hour_bucket = ?`, identityID, hourBucket,
		).Error
	})

	return refunded, err
}
