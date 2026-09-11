package main

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type pingRow struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
}

// TableName pins the table to the name the migration created, overriding
// gorm's default pluralization ("ping_rows") for this struct name.
func (pingRow) TableName() string {
	return "pings"
}

type pingResponse struct {
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// pingHandler writes a heartbeat row to confirm the DB round trip works, then returns pong.
// @Router /ping [get]
func pingHandler(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		p := pingRow{}
		if err := db.Create(&p).Error; err != nil {
			c.JSON(http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, pingResponse{Message: "pong", Time: p.CreatedAt})
	}
}
