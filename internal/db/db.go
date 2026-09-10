package db

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"pigeon/internal/config"
)

func Connect(cfg config.Config) (*gorm.DB, error) {
	return gorm.Open(postgres.Open(cfg.DSN()), &gorm.Config{})
}
