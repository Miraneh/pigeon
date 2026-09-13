package operator

import (
	"context"
	"errors"
	"testing"
	"time"

	"pigeon/server/dispatch"
)

func TestFake_Send_Succeeds(t *testing.T) {
	op := &Fake{failRate: 0, latency: time.Millisecond}

	if err := op.Send(context.Background(), []dispatch.Item{{}}); err != nil {
		t.Fatalf("Send() = %v, want nil", err)
	}
}

func TestFake_Send_AlwaysFailsAtFullFailRate(t *testing.T) {
	op := &Fake{failRate: 1, latency: time.Millisecond}

	for range 20 {
		if err := op.Send(context.Background(), nil); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Send() = %v, want %v", err, ErrUnavailable)
		}
	}
}

func TestFake_Send_ReturnsCtxErrOnCancellation(t *testing.T) {
	op := &Fake{failRate: 0, latency: time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := op.Send(ctx, nil)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() = %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed >= op.latency {
		t.Fatalf("Send() took %v, expected it to return well before the %v latency elapsed", elapsed, op.latency)
	}
}
