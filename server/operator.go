package main

import (
	"context"
	"errors"
	"flag"
	"math/rand"
	"time"
)

var (
	operatorFailRateFlag = flag.Float64(
		"operator-fail-rate", 0, "fake operator: probability of failure, for exercising the retry path",
	)
	operatorLatencyFlag = flag.Duration(
		"operator-latency", 20*time.Millisecond, "fake operator: simulated response latency",
	)
)

var errOperatorUnavailable = errors.New("operator: temporarily unavailable")

// operatorClient is what the dispatcher sends confirmed-paid batches to.
type operatorClient interface {
	Send(ctx context.Context, batch []dispatchItem) error
}

// fakeOperator simulates a downstream SMS operator that takes `latency` to respond and fails at `failRate`.
type fakeOperator struct {
	failRate float64
	latency  time.Duration
}

func newFakeOperator() *fakeOperator {
	return &fakeOperator{failRate: *operatorFailRateFlag, latency: *operatorLatencyFlag}
}

func (o *fakeOperator) Send(ctx context.Context, _ []dispatchItem) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(o.latency):
	}

	if o.failRate > 0 && rand.Float64() < o.failRate { //nolint:gosec // not a security-relevant use of rand
		return errOperatorUnavailable
	}

	return nil
}
