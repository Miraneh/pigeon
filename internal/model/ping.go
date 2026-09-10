package model

import "time"

type Ping struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
}
