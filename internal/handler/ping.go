package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"pigeon/internal/model"
)

type PingResponse struct {
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

// Ping writes a heartbeat row to confirm the DB round trip works, then returns pong.
func Ping(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		p := model.Ping{}
		if err := db.Create(&p).Error; err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, PingResponse{Message: "pong", Time: p.CreatedAt})
	}
}
