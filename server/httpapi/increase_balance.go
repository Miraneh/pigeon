package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

const (
	idempotencyKeyHeader     = "Idempotency-Key"
	maxIdempotencyKeyLen     = 255
	pgNumericValueOutOfRange = "22003"
)

var (
	errIdempotencyKeyReused = errors.New("idempotency key already used with a different amount")
	errBalanceOverflow      = errors.New("increase would overflow the balance")
)

type increaseBalanceRequest struct {
	Amount int64 `binding:"required,gt=0" json:"amount"`
}

type increaseBalanceResponse struct {
	IdentityID int64 `json:"identity_id"`
	Amount     int64 `json:"amount"`
	Balance    int64 `json:"balance"`
}

// identityIncreaseBalance increases an identity's balance, at most once per Idempotency-Key.
// @Router /identities/{id}/balance/increase [post]
func identityIncreaseBalance(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid id"})
			return
		}

		key := c.GetHeader(idempotencyKeyHeader)
		if key == "" || len(key) > maxIdempotencyKeyLen {
			c.JSON(http.StatusBadRequest, errorResponse{Error: "Idempotency-Key header is required, up to 255 bytes"})
			return
		}

		var body increaseBalanceRequest
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{Error: err.Error()})
			return
		}

		res, err := increaseBalance(c.Request.Context(), db, id, key, body.Amount)
		switch {
		case errors.Is(err, errIdempotencyKeyReused), errors.Is(err, errBalanceOverflow):
			c.JSON(http.StatusUnprocessableEntity, errorResponse{Error: err.Error()})
		case err != nil:
			c.JSON(http.StatusInternalServerError, errorResponse{Error: err.Error()})
		default:
			c.JSON(http.StatusOK, res)
		}
	}
}

// increaseBalance adds amount to the identity's balance and records the increase under idempotencyKey, in one
// transaction.
func increaseBalance(
	ctx context.Context, db *gorm.DB, identityID int64, idempotencyKey string, amount int64,
) (increaseBalanceResponse, error) {
	res := increaseBalanceResponse{IdentityID: identityID, Amount: amount}

	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			`INSERT INTO identities (id, balance) VALUES (?, 0) ON CONFLICT (id) DO NOTHING`, identityID,
		).Error; err != nil {
			return err
		}

		var balance int64
		if err := tx.Raw(
			`SELECT balance FROM identities WHERE id = ? FOR UPDATE`, identityID,
		).Scan(&balance).Error; err != nil {
			return err
		}

		var prev []struct {
			Amount       int64
			BalanceAfter int64
		}
		if err := tx.Raw(
			`SELECT amount, balance_after FROM balance_increases WHERE identity_id = ? AND idempotency_key = ?`,
			identityID, idempotencyKey,
		).Scan(&prev).Error; err != nil {
			return err
		}
		if len(prev) > 0 {
			if prev[0].Amount != amount {
				return errIdempotencyKeyReused
			}
			res.Balance = prev[0].BalanceAfter
			return nil
		}

		if err := tx.Raw(
			`UPDATE identities SET balance = balance + ? WHERE id = ? RETURNING balance`, amount, identityID,
		).Scan(&res.Balance).Error; err != nil {
			return err
		}

		return tx.Exec(
			`INSERT INTO balance_increases (identity_id, idempotency_key, amount, balance_after) VALUES (?, ?, ?, ?)`,
			identityID, idempotencyKey, amount, res.Balance,
		).Error
	})

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgNumericValueOutOfRange {
		return increaseBalanceResponse{}, errBalanceOverflow
	}
	if err != nil {
		return increaseBalanceResponse{}, err
	}

	return res, nil
}
