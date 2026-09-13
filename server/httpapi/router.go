// Package httpapi exposes the SMS gateway's HTTP endpoints.
package httpapi

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	swaggerfiles "github.com/swaggo/files"
	ginswagger "github.com/swaggo/gin-swagger"
	"gorm.io/gorm"

	_ "pigeon/docs"
	"pigeon/server/admission"
)

type sendHandler struct {
	admitter *admission.Admitter
	timeout  time.Duration
}

// sendMessages queues the request with the admission batcher and blocks until that batch has been locked against
// the identity's balance.
// @Router /messages [post]
func (h *sendHandler) sendMessages(c *gin.Context) {
	var body sendRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	msgs := make([]admission.Message, len(body.Messages))
	for i, m := range body.Messages {
		msgs[i] = admission.Message{To: m.To, Text: m.Text}
	}

	req := &admission.Request{
		IdentityID: body.IdentityID,
		Express:    body.Express,
		Messages:   msgs,
		ReceivedAt: time.Now(),
		ResultCh:   make(chan admission.Result, 1),
	}
	h.admitter.Submit(req)

	ctx, cancel := context.WithTimeout(c.Request.Context(), h.timeout)
	defer cancel()

	select {
	case res := <-req.ResultCh:
		c.JSON(http.StatusOK, res)
	case <-ctx.Done():
		c.JSON(http.StatusGatewayTimeout, errorResponse{Error: "timed out waiting for admission"})
	}
}

// pingHandler is a liveness check.
// @Router /ping [get]
func pingHandler(c *gin.Context) {
	c.JSON(http.StatusOK, pingResponse{Message: "pong", Time: time.Now()})
}

// identityBalance is a read-only helper until the user/ component exists to manage balances properly.
// @Router /identities/{id} [get]
func identityBalance(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid id"})
			return
		}

		var balance int64
		if err := db.Raw(`SELECT balance FROM identities WHERE id = ?`, id).Scan(&balance).Error; err != nil {
			c.JSON(http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"identity_id": id, "balance": balance})
	}
}

// NewRouter wires the HTTP routes. sendTimeout bounds how long POST /messages waits for its batch result.
func NewRouter(db *gorm.DB, admitter *admission.Admitter, sendTimeout time.Duration) *gin.Engine {
	r := gin.Default()

	send := &sendHandler{admitter: admitter, timeout: sendTimeout}

	r.POST("/messages", send.sendMessages)
	r.GET("/ping", pingHandler)
	r.GET("/identities/:id", identityBalance(db))
	r.POST("/identities/:id/balance/increase", identityIncreaseBalance(db))
	r.GET("/identities/:id/report", identityReport(db))
	r.GET("/swagger/*any", ginswagger.WrapHandler(swaggerfiles.Handler))

	return r
}
