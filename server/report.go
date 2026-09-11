package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type reportBucket struct {
	TimeBucket   time.Time `json:"time_bucket"`
	MessagesSent int64     `json:"messages_sent"`
	AmountSpent  int64     `json:"amount_spent"`
}

type reportTotals struct {
	MessagesSent int64 `json:"messages_sent"`
	AmountSpent  int64 `json:"amount_spent"`
}

type reportResponse struct {
	IdentityID int64          `json:"identity_id"`
	Start      time.Time      `json:"start"`
	End        time.Time      `json:"end"`
	Buckets    []reportBucket `json:"buckets"`
	Total      reportTotals   `json:"total"`
}

// identityReport returns hourly-bucketed send/spend stats for an identity over [start, end], sorted by time_bucket,
// plus a total across the range. Buckets with no activity are simply absent, not zero-filled.
// @Router /identities/{id}/report [get]
func identityReport(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid id"})
			return
		}

		start, err := time.Parse(time.RFC3339, c.Query("start"))
		if err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid start, expected RFC3339"})
			return
		}

		end, err := time.Parse(time.RFC3339, c.Query("end"))
		if err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid end, expected RFC3339"})
			return
		}

		if end.Before(start) {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "end must not be before start"})
			return
		}

		var buckets []reportBucket
		err = db.Raw(`
			SELECT hour_bucket AS time_bucket, messages_sent, amount_spent
			FROM report_stats
			WHERE identity_id = ? AND hour_bucket >= ? AND hour_bucket <= ?
			ORDER BY hour_bucket ASC
		`, id, start, end).Scan(&buckets).Error
		if err != nil {
			c.JSON(http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}

		var total reportTotals
		for _, b := range buckets {
			total.MessagesSent += b.MessagesSent
			total.AmountSpent += b.AmountSpent
		}

		c.JSON(http.StatusOK, reportResponse{
			IdentityID: id,
			Start:      start,
			End:        end,
			Buckets:    buckets,
			Total:      total,
		})
	}
}
