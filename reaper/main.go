package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"pigeon/internal/database"
	"pigeon/reaper/reap"
)

var (
	dbConfig = database.RegisterFlags(flag.CommandLine)

	expiryFlag = flag.Duration(
		"expiry", 3*time.Hour, "balance_locks with hour_bucket older than this are refunded and cleared",
	)
)

// reaper is a cronjob, meant to be invoked hourly and runs a single reap against whatever's stale and exits.
func main() {
	flag.Parse()

	db, err := database.Open(*dbConfig)
	if err != nil {
		slog.Error("connect db", "error", err)
		os.Exit(1)
	}

	refunded, err := reap.Run(context.Background(), db, *expiryFlag)
	if err != nil {
		slog.Error("reap", "error", err)
		os.Exit(1)
	}

	slog.Info("reap complete", "refunded_locks", refunded)
}
