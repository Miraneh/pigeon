package main

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func connectDB(cfg config) (*gorm.DB, error) {
	return gorm.Open(postgres.Open(cfg.dsn()), &gorm.Config{})
}
