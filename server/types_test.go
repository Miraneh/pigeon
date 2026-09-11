package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func bindJSON(t *testing.T, body string, target any) error {
	t.Helper()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")
	return c.ShouldBindJSON(target)
}

func TestSendRequest_Binding(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"valid", `{"identity_id":1,"messages":[{"to":"a","text":"hi"}]}`, false},
		{"missing identity_id", `{"messages":[{"to":"a","text":"hi"}]}`, true},
		{"empty messages", `{"identity_id":1,"messages":[]}`, true},
		{"missing messages", `{"identity_id":1}`, true},
		// messageIn's own `binding:"required"` tags are never evaluated: Messages lacks a `dive` tag, so
		// go-playground/validator validates the slice itself (required, min=1) but not its elements' fields.
		{"message missing to (not actually validated)", `{"identity_id":1,"messages":[{"text":"hi"}]}`, false},
		{"message missing text (not actually validated)", `{"identity_id":1,"messages":[{"to":"a"}]}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req sendRequest
			err := bindJSON(t, tt.body, &req)
			if (err != nil) != tt.wantErr {
				t.Errorf("bind error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSendRequest_ExpressDefaultsFalse(t *testing.T) {
	var req sendRequest
	if err := bindJSON(t, `{"identity_id":1,"messages":[{"to":"a","text":"hi"}]}`, &req); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if req.Express {
		t.Error("Express = true, want false when omitted")
	}
}

func TestTopupRequest_Binding(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"valid", `{"amount":10}`, false},
		{"zero rejected", `{"amount":0}`, true},
		{"negative rejected", `{"amount":-5}`, true},
		{"missing amount", `{}`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req topupRequest
			err := bindJSON(t, tt.body, &req)
			if (err != nil) != tt.wantErr {
				t.Errorf("bind error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
