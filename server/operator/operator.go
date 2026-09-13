// Package operator provides a simulated downstream SMS operator.
package operator

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"pigeon/server/dispatch"
)

var ErrUnavailable = errors.New("operator: temporarily unavailable")

// Fake simulates a downstream SMS operator that takes `latency` to respond and fails at `failRate`.
type Fake struct {
	failRate float64
	latency  time.Duration
}

func NewFake(failRate float64, latency time.Duration) *Fake {
	return &Fake{failRate: failRate, latency: latency}
}

func (o *Fake) Send(ctx context.Context, _ []dispatch.Item) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(o.latency):
	}

	if o.failRate > 0 && rand.Float64() < o.failRate { //nolint:gosec // not a security-relevant use of rand
		return ErrUnavailable
	}

	return nil
}
