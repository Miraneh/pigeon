package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func doReportRequest(t *testing.T, r http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestIdentityReport_ReturnsBucketsSortedWithTotal(t *testing.T) {
	db := openTestDB(t)
	seedIdentity(t, db, 1, 0)
	seedIdentity(t, db, 2, 0)

	base := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	before, first, second := base, base.Add(time.Hour), base.Add(2*time.Hour)

	seedReportStats(t, db, 1, before, 100, 1000) // outside the query window
	seedReportStats(t, db, 1, second, 5, 50)     // seeded out of order on purpose
	seedReportStats(t, db, 1, first, 3, 30)
	seedReportStats(t, db, 2, first, 9, 90) // different identity, must not leak in

	r := newRouter(db, newAdmitterForTest(db, time.Hour, 100))
	path := fmt.Sprintf("/identities/1/report?start=%s&end=%s",
		url.QueryEscape(first.Format(time.RFC3339)), url.QueryEscape(second.Format(time.RFC3339)))
	w := doReportRequest(t, r, path)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp reportResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.IdentityID != 1 {
		t.Errorf("IdentityID = %d, want 1", resp.IdentityID)
	}
	if len(resp.Buckets) != 2 {
		t.Fatalf("len(Buckets) = %d, want 2: %+v", len(resp.Buckets), resp.Buckets)
	}
	if !resp.Buckets[0].TimeBucket.Equal(first) || !resp.Buckets[1].TimeBucket.Equal(second) {
		t.Errorf("buckets not sorted by time: %+v", resp.Buckets)
	}
	if resp.Buckets[0].MessagesSent != 3 || resp.Buckets[0].AmountSpent != 30 {
		t.Errorf("bucket[0] = %+v, want sent=3 amount=30", resp.Buckets[0])
	}
	if resp.Buckets[1].MessagesSent != 5 || resp.Buckets[1].AmountSpent != 50 {
		t.Errorf("bucket[1] = %+v, want sent=5 amount=50", resp.Buckets[1])
	}
	if resp.Total.MessagesSent != 8 || resp.Total.AmountSpent != 80 {
		t.Errorf("Total = %+v, want sent=8 amount=80", resp.Total)
	}
}

func TestIdentityReport_NoActivityReturnsEmptyBuckets(t *testing.T) {
	db := openTestDB(t)
	seedIdentity(t, db, 1, 0)

	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	r := newRouter(db, newAdmitterForTest(db, time.Hour, 100))
	path := fmt.Sprintf("/identities/1/report?start=%s&end=%s",
		url.QueryEscape(start.Format(time.RFC3339)), url.QueryEscape(end.Format(time.RFC3339)))
	w := doReportRequest(t, r, path)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp reportResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(resp.Buckets) != 0 {
		t.Errorf("len(Buckets) = %d, want 0: %+v", len(resp.Buckets), resp.Buckets)
	}
	if resp.Total.MessagesSent != 0 || resp.Total.AmountSpent != 0 {
		t.Errorf("Total = %+v, want zero", resp.Total)
	}
}

func TestIdentityReport_InvalidID(t *testing.T) {
	r := newRouter(nil, newAdmitterForTest(nil, time.Hour, 100))
	w := doReportRequest(t, r, "/identities/not-a-number/report?start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:00Z")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestIdentityReport_InvalidStart(t *testing.T) {
	r := newRouter(nil, newAdmitterForTest(nil, time.Hour, 100))
	w := doReportRequest(t, r, "/identities/1/report?start=not-a-time&end=2026-01-02T00:00:00Z")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestIdentityReport_InvalidEnd(t *testing.T) {
	r := newRouter(nil, newAdmitterForTest(nil, time.Hour, 100))
	w := doReportRequest(t, r, "/identities/1/report?start=2026-01-01T00:00:00Z&end=not-a-time")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestIdentityReport_EndBeforeStartRejected(t *testing.T) {
	r := newRouter(nil, newAdmitterForTest(nil, time.Hour, 100))
	w := doReportRequest(t, r, "/identities/1/report?start=2026-01-02T00:00:00Z&end=2026-01-01T00:00:00Z")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}
