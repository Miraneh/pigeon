package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	swaggerfiles "github.com/swaggo/files"
	ginswagger "github.com/swaggo/gin-swagger"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	_ "pigeon/docs"
)

var (
	appPortFlag = flag.String("app-port", "8080", "HTTP listen port")

	dbHostFlag     = flag.String("db-host", "localhost", "Postgres host")
	dbPortFlag     = flag.String("db-port", "5432", "Postgres port")
	dbUserFlag     = flag.String("db-user", "pigeon", "Postgres user")
	dbPasswordFlag = flag.String("db-password", "pigeon", "Postgres password")
	dbNameFlag     = flag.String("db-name", "pigeon", "Postgres database name")
	dbSSLModeFlag  = flag.String("db-sslmode", "disable", "Postgres sslmode")

	dispatchBufferFlag  = flag.Int("dispatch-buffer", 100_000, "express/regular channel buffer size")
	dispatchTimeoutFlag = flag.Duration(
		"dispatch-timeout", 10*time.Second, "how long POST /messages waits for its batch result",
	)
)

// runner is implemented by admitter and dispatcher: both drain their input and flush whatever's pending before
// returning once ctx is cancelled.
type runner interface {
	run(context.Context)
}

func dbDSN() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		*dbHostFlag, *dbPortFlag, *dbUserFlag, *dbPasswordFlag, *dbNameFlag, *dbSSLModeFlag)
}

// dbURL is the URL form that golang-migrate expects.
func dbURL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		*dbUserFlag, *dbPasswordFlag, *dbHostFlag, *dbPortFlag, *dbNameFlag, *dbSSLModeFlag)
}

type sendHandler struct {
	admission *admitter
	timeout   time.Duration
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

	msgs := make([]admissionMsg, len(body.Messages))
	for i, m := range body.Messages {
		msgs[i] = admissionMsg{to: m.To, text: m.Text}
	}

	req := &admissionRequest{
		identityID: body.IdentityID,
		express:    body.Express,
		messages:   msgs,
		receivedAt: time.Now(),
		resultCh:   make(chan sendResult, 1),
	}
	h.admission.submit(req)

	ctx, cancel := context.WithTimeout(c.Request.Context(), h.timeout)
	defer cancel()

	select {
	case res := <-req.resultCh:
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

type topupRequest struct {
	Amount int64 `binding:"required,gt=0" json:"amount"`
}

// identityTopup is a helper for crediting balance until the user/ component exists.
// @Router /identities/{id}/topup [post]
func identityTopup(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid id"})
			return
		}

		var body topupRequest
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{Error: err.Error()})
			return
		}

		err = db.Exec(`
			INSERT INTO identities (id, balance) VALUES (?, ?)
			ON CONFLICT (id) DO UPDATE SET balance = identities.balance + EXCLUDED.balance
		`, id, body.Amount).Error
		if err != nil {
			c.JSON(http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"identity_id": id, "credited": body.Amount})
	}
}

func newRouter(db *gorm.DB, admission *admitter) *gin.Engine {
	r := gin.Default()

	send := &sendHandler{admission: admission, timeout: *dispatchTimeoutFlag}

	r.POST("/messages", send.sendMessages)
	r.GET("/ping", pingHandler)
	r.GET("/identities/:id", identityBalance(db))
	r.POST("/identities/:id/topup", identityTopup(db))
	r.GET("/swagger/*any", ginswagger.WrapHandler(swaggerfiles.Handler))

	return r
}

func main() {
	flag.Parse()

	if err := runMigrations(dbURL()); err != nil {
		slog.Error("run migrations", "error", err)
		os.Exit(1)
	}

	db, err := gorm.Open(postgres.Open(dbDSN()), &gorm.Config{})
	if err != nil {
		slog.Error("connect db", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	expressCh := make(chan dispatchItem, *dispatchBufferFlag)
	regularCh := make(chan dispatchItem, *dispatchBufferFlag)

	admission := newAdmitter(db, expressCh, regularCh)
	operator := newFakeOperator()
	express := newDispatcher("express", expressCh, *expressWindowFlag, *expressMaxSizeFlag, db, operator)
	regular := newDispatcher("regular", regularCh, *regularWindowFlag, *regularMaxSizeFlag, db, operator)

	// Every pipeline stage flushes its pending work when ctx is cancelled;
	// we block on pipeline.Wait() below so the process can't exit mid-flush.
	var pipeline sync.WaitGroup
	for _, r := range []runner{admission, express, regular} {
		pipeline.Add(1)
		go func(r runner) {
			defer pipeline.Done()
			r.run(ctx)
		}(r)
	}

	srv := &http.Server{
		Addr:              ":" + *appPortFlag,
		Handler:           newRouter(db, admission),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("http shutdown", "error", err)
		}
	}()

	slog.Info("listening", "port", *appPortFlag)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("run server", "error", err)
		os.Exit(1)
	}

	pipeline.Wait()
	slog.Info("pipeline drained, exiting")
}
