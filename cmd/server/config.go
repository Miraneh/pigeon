package main

import (
	"fmt"
	"os"
)

type config struct {
	appPort    string
	dbHost     string
	dbPort     string
	dbUser     string
	dbPassword string
	dbName     string
	dbSSLMode  string
}

func loadConfig() config {
	return config{
		appPort:    getEnv("APP_PORT", "8080"),
		dbHost:     getEnv("DB_HOST", "localhost"),
		dbPort:     getEnv("DB_PORT", "5432"),
		dbUser:     getEnv("DB_USER", "pigeon"),
		dbPassword: getEnv("DB_PASSWORD", "pigeon"),
		dbName:     getEnv("DB_NAME", "pigeon"),
		dbSSLMode:  getEnv("DB_SSLMODE", "disable"),
	}
}

func (c config) dsn() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.dbHost, c.dbPort, c.dbUser, c.dbPassword, c.dbName, c.dbSSLMode)
}

// databaseURL is the URL form golang-migrate expects, as opposed to the libpq keyword form
// gorm's postgres driver takes.
func (c config) databaseURL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.dbUser, c.dbPassword, c.dbHost, c.dbPort, c.dbName, c.dbSSLMode)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
