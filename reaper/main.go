package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	dbHostFlag     = flag.String("db-host", "localhost", "Postgres host")
	dbPortFlag     = flag.String("db-port", "5432", "Postgres port")
	dbUserFlag     = flag.String("db-user", "pigeon", "Postgres user")
	dbPasswordFlag = flag.String("db-password", "pigeon", "Postgres password")
	dbNameFlag     = flag.String("db-name", "pigeon", "Postgres database name")
	dbSSLModeFlag  = flag.String("db-sslmode", "disable", "Postgres sslmode")

	expiryFlag = flag.Duration(
		"expiry", 3*time.Hour, "balance_locks with hour_bucket older than this are refunded and cleared",
	)
)

func dbDSN() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		*dbHostFlag, *dbPortFlag, *dbUserFlag, *dbPasswordFlag, *dbNameFlag, *dbSSLModeFlag)
}

// reaper is a cronjob, meant to be invoked hourly and runs a single reap against whatever's stale and exits.
func main() {
	flag.Parse()

	db, err := gorm.Open(postgres.Open(dbDSN()), &gorm.Config{})
	if err != nil {
		slog.Error("connect db", "error", err)
		os.Exit(1)
	}

	refunded, err := reap(context.Background(), db, *expiryFlag)
	if err != nil {
		slog.Error("reap", "error", err)
		os.Exit(1)
	}

	slog.Info("reap complete", "refunded_locks", refunded)
}
