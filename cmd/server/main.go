package main

import "log"

func main() {
	cfg := loadConfig()

	if err := runMigrations(cfg.databaseURL()); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	db, err := connectDB(cfg)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}

	r := newRouter(db)

	if err := r.Run(":" + cfg.appPort); err != nil {
		log.Fatalf("run server: %v", err)
	}
}
