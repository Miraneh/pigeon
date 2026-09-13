// Package database holds the Postgres connection settings and migration runner shared by the pigeon binaries.
package database

import (
	"flag"
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
}

// RegisterFlags registers the Postgres flags on fs and returns the Config they populate once fs is parsed.
func RegisterFlags(fs *flag.FlagSet) *Config {
	c := &Config{}
	fs.StringVar(&c.Host, "db-host", "localhost", "Postgres host")
	fs.StringVar(&c.Port, "db-port", "5432", "Postgres port")
	fs.StringVar(&c.User, "db-user", "pigeon", "Postgres user")
	fs.StringVar(&c.Password, "db-password", "pigeon", "Postgres password")
	fs.StringVar(&c.Name, "db-name", "pigeon", "Postgres database name")
	fs.StringVar(&c.SSLMode, "db-sslmode", "disable", "Postgres sslmode")
	return c
}

func (c Config) DSN() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Name, c.SSLMode)
}

// URL is the URL form that golang-migrate expects.
func (c Config) URL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Name, c.SSLMode)
}

func Open(c Config) (*gorm.DB, error) {
	return gorm.Open(postgres.Open(c.DSN()), &gorm.Config{})
}
