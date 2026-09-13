package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"pigeon/internal/database"
	"pigeon/server/admission"
	"pigeon/server/dispatch"
	"pigeon/server/httpapi"
	"pigeon/server/operator"
)

var (
	appPortFlag = flag.String("app-port", "8080", "HTTP listen port")

	dbConfig = database.RegisterFlags(flag.CommandLine)

	dispatchBufferFlag  = flag.Int("dispatch-buffer", 100_000, "express/regular channel buffer size")
	dispatchTimeoutFlag = flag.Duration(
		"dispatch-timeout", 10*time.Second, "how long POST /messages waits for its batch result",
	)

	admissionWindowFlag  = flag.Duration("admission-window", 50*time.Millisecond, "admission batch flush interval")
	admissionMaxSizeFlag = flag.Int("admission-max-size", 5000, "admission batch max size before an early flush")
	pricePerSMSFlag      = flag.Int64("price-per-sms", 1, "cost of one SMS, in balance units")

	expressWindowFlag  = flag.Duration("express-window", 500*time.Millisecond, "express dispatch flush interval")
	expressMaxSizeFlag = flag.Int("express-max-size", 300, "express dispatch max batch size")
	regularWindowFlag  = flag.Duration("regular-window", time.Second, "regular dispatch flush interval")
	regularMaxSizeFlag = flag.Int("regular-max-size", 600, "regular dispatch max batch size")

	expiryFlag = flag.Duration(
		"expiry", 3*time.Hour, "how long a message may wait before being dropped undelivered",
	)
	operatorTimeoutFlag = flag.Duration(
		"operator-timeout", 500*time.Millisecond, "bound on one operator call, well above its own p99.9",
	)

	operatorFailRateFlag = flag.Float64(
		"operator-fail-rate", 0, "fake operator: probability of failure, for exercising the retry path",
	)
	operatorLatencyFlag = flag.Duration(
		"operator-latency", 20*time.Millisecond, "fake operator: simulated response latency",
	)
)

// runner is implemented by admission.Admitter and dispatch.Dispatcher: both drain their input and flush whatever's
// pending before returning once ctx is cancelled.
type runner interface {
	Run(ctx context.Context)
}

func main() {
	flag.Parse()

	if err := database.RunMigrations(dbConfig.URL()); err != nil {
		slog.Error("run migrations", "error", err)
		os.Exit(1)
	}

	db, err := database.Open(*dbConfig)
	if err != nil {
		slog.Error("connect db", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	expressCh := make(chan dispatch.Item, *dispatchBufferFlag)
	regularCh := make(chan dispatch.Item, *dispatchBufferFlag)

	admitter := admission.New(admission.Config{
		Window:  *admissionWindowFlag,
		MaxSize: *admissionMaxSizeFlag,
		Price:   *pricePerSMSFlag,
	}, db, expressCh, regularCh)

	op := operator.NewFake(*operatorFailRateFlag, *operatorLatencyFlag)
	express := dispatch.New("express", dispatch.Config{
		Window:          *expressWindowFlag,
		MaxSize:         *expressMaxSizeFlag,
		Expiry:          *expiryFlag,
		OperatorTimeout: *operatorTimeoutFlag,
	}, expressCh, db, op)
	regular := dispatch.New("regular", dispatch.Config{
		Window:          *regularWindowFlag,
		MaxSize:         *regularMaxSizeFlag,
		Expiry:          *expiryFlag,
		OperatorTimeout: *operatorTimeoutFlag,
	}, regularCh, db, op)

	// Every pipeline stage flushes its pending work when ctx is cancelled;
	// we block on pipeline.Wait() below so the process can't exit mid-flush.
	var pipeline sync.WaitGroup
	for _, r := range []runner{admitter, express, regular} {
		pipeline.Add(1)
		go func(r runner) {
			defer pipeline.Done()
			r.Run(ctx)
		}(r)
	}

	srv := &http.Server{
		Addr:              ":" + *appPortFlag,
		Handler:           httpapi.NewRouter(db, admitter, *dispatchTimeoutFlag),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("http shutdown", "error", err)
		}
	}()

	slog.Info("listening", "port", *appPortFlag)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("run server", "error", err)
		os.Exit(1)
	}

	pipeline.Wait()
	slog.Info("pipeline drained, exiting")
}
