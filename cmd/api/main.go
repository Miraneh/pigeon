// Package main is the API server entrypoint.
package main

import (
	"log"

	"pigeon/internal/config"
	"pigeon/internal/db"
	"pigeon/internal/router"
)

func main() {
	cfg := config.Load()

	if err := db.RunMigrations(cfg.DatabaseURL()); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	gormDB, err := db.Connect(cfg)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}

	r := router.New(gormDB)

	if err := r.Run(":" + cfg.AppPort); err != nil {
		log.Fatalf("run server: %v", err)
	}
}
