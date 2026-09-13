package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"pigeon/internal/dbtest"
	"pigeon/server/admission"
	"pigeon/server/dispatch"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestMain(m *testing.M) {
	os.Exit(dbtest.Main(m))
}

// newTestAdmitter builds an admitter with real channels and a fixed price of 1.
func newTestAdmitter(db *gorm.DB, maxSize int) *admission.Admitter {
	return admission.New(
		admission.Config{Window: time.Hour, MaxSize: maxSize, Price: 1}, db,
		make(chan dispatch.Item, 100), make(chan dispatch.Item, 100),
	)
}

func newTestRouter(db *gorm.DB) *gin.Engine {
	return NewRouter(db, newTestAdmitter(db, 100), 10*time.Second)
}

func TestPingHandler(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ping", nil)

	pingHandler(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp pingResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Message != "pong" {
		t.Errorf("Message = %q, want %q", resp.Message, "pong")
	}
	if time.Since(resp.Time) > time.Minute {
		t.Errorf("Time = %v, want close to now", resp.Time)
	}
}

func TestSendHandler_InvalidJSONReturnsBadRequest(t *testing.T) {
	h := &sendHandler{admitter: newTestAdmitter(nil, 100), timeout: time.Second}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(
		context.Background(), http.MethodPost, "/messages", bytes.NewBufferString(`{"identity_id": 1}`),
	)
	c.Request.Header.Set("Content-Type", "application/json")

	h.sendMessages(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestSendHandler_TimesOutWaitingForAdmission(t *testing.T) {
	h := &sendHandler{admitter: newTestAdmitter(nil, 1_000_000), timeout: 20 * time.Millisecond}

	body, err := json.Marshal(sendRequest{IdentityID: 1, Messages: []messageIn{{To: "x", Text: "y"}}})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(
		context.Background(), http.MethodPost, "/messages", bytes.NewReader(body),
	)
	c.Request.Header.Set("Content-Type", "application/json")

	h.sendMessages(c)

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusGatewayTimeout)
	}
}

func TestNewRouter_PingRoute(t *testing.T) {
	r := newTestRouter(nil)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET /ping status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestIdentityBalance_ReturnsBalance(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 42, 250)

	r := newTestRouter(db)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/identities/42", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusOK, w.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got, ok := body["balance"].(float64); !ok || got != 250 {
		t.Errorf("balance = %v, want 250", body["balance"])
	}
}

func TestIdentityBalance_InvalidID(t *testing.T) {
	r := newTestRouter(nil)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/identities/not-a-number", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestIdentityTopup_CreditsNewIdentity(t *testing.T) {
	db := dbtest.Open(t)
	r := newTestRouter(db)

	body, err := json.Marshal(topupRequest{Amount: 50})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequestWithContext(
		context.Background(), http.MethodPost, "/identities/7/topup", bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusOK, w.Body.String())
	}
	if got := dbtest.IdentityBalance(t, db, 7); got != 50 {
		t.Errorf("identity balance = %d, want 50", got)
	}
}

func TestIdentityTopup_AddsToExistingBalance(t *testing.T) {
	db := dbtest.Open(t)
	dbtest.SeedIdentity(t, db, 7, 100)
	r := newTestRouter(db)

	body, err := json.Marshal(topupRequest{Amount: 50})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequestWithContext(
		context.Background(), http.MethodPost, "/identities/7/topup", bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := dbtest.IdentityBalance(t, db, 7); got != 150 {
		t.Errorf("identity balance = %d, want 150", got)
	}
}

func TestIdentityTopup_RejectsNonPositiveAmount(t *testing.T) {
	r := newTestRouter(nil)

	body, err := json.Marshal(topupRequest{Amount: 0})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequestWithContext(
		context.Background(), http.MethodPost, "/identities/7/topup", bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}
