package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func withFlagValue(t *testing.T, flagVar *string, value string) {
	t.Helper()
	orig := *flagVar
	*flagVar = value
	t.Cleanup(func() { *flagVar = orig })
}

func TestDBDSN(t *testing.T) {
	withFlagValue(t, dbHostFlag, "dbhost")
	withFlagValue(t, dbPortFlag, "1111")
	withFlagValue(t, dbUserFlag, "u")
	withFlagValue(t, dbPasswordFlag, "p")
	withFlagValue(t, dbNameFlag, "n")
	withFlagValue(t, dbSSLModeFlag, "require")

	want := "host=dbhost port=1111 user=u password=p dbname=n sslmode=require"
	if got := dbDSN(); got != want {
		t.Errorf("dbDSN() = %q, want %q", got, want)
	}
}

func TestDBURL(t *testing.T) {
	withFlagValue(t, dbHostFlag, "dbhost")
	withFlagValue(t, dbPortFlag, "1111")
	withFlagValue(t, dbUserFlag, "u")
	withFlagValue(t, dbPasswordFlag, "p")
	withFlagValue(t, dbNameFlag, "n")
	withFlagValue(t, dbSSLModeFlag, "require")

	want := "postgres://u:p@dbhost:1111/n?sslmode=require"
	if got := dbURL(); got != want {
		t.Errorf("dbURL() = %q, want %q", got, want)
	}
}

func TestPingHandler(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/ping", nil)

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
	h := &sendHandler{admission: newAdmitterForTest(nil, time.Hour, 100), timeout: time.Second}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/messages", bytes.NewBufferString(`{"identity_id": 1}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.sendMessages(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestSendHandler_TimesOutWaitingForAdmission(t *testing.T) {
	b := newAdmitterForTest(nil, time.Hour, 1_000_000) // run() never started, so nothing drains `in`
	h := &sendHandler{admission: b, timeout: 20 * time.Millisecond}

	body, err := json.Marshal(sendRequest{IdentityID: 1, Messages: []messageIn{{To: "x", Text: "y"}}})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	h.sendMessages(c)

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusGatewayTimeout)
	}
}

func TestNewRouter_PingRoute(t *testing.T) {
	r := newRouter(nil, newAdmitterForTest(nil, time.Hour, 100))

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET /ping status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestIdentityBalance_ReturnsBalance(t *testing.T) {
	db := openTestDB(t)
	seedIdentity(t, db, 42, 250)

	r := newRouter(db, newAdmitterForTest(db, time.Hour, 100))
	req := httptest.NewRequest(http.MethodGet, "/identities/42", nil)
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
	r := newRouter(nil, newAdmitterForTest(nil, time.Hour, 100))
	req := httptest.NewRequest(http.MethodGet, "/identities/not-a-number", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestIdentityTopup_CreditsNewIdentity(t *testing.T) {
	db := openTestDB(t)
	r := newRouter(db, newAdmitterForTest(db, time.Hour, 100))

	body, err := json.Marshal(topupRequest{Amount: 50})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/identities/7/topup", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusOK, w.Body.String())
	}
	if got := identityBalanceOf(t, db, 7); got != 50 {
		t.Errorf("identity balance = %d, want 50", got)
	}
}

func TestIdentityTopup_AddsToExistingBalance(t *testing.T) {
	db := openTestDB(t)
	seedIdentity(t, db, 7, 100)
	r := newRouter(db, newAdmitterForTest(db, time.Hour, 100))

	body, err := json.Marshal(topupRequest{Amount: 50})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/identities/7/topup", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := identityBalanceOf(t, db, 7); got != 150 {
		t.Errorf("identity balance = %d, want 150", got)
	}
}

func TestIdentityTopup_RejectsNonPositiveAmount(t *testing.T) {
	r := newRouter(nil, newAdmitterForTest(nil, time.Hour, 100))

	body, err := json.Marshal(topupRequest{Amount: 0})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/identities/7/topup", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}
