package main

import "time"

const (
	statusOK                         = "ok"
	statusErrPartiallySentBalanceHit = "err_partially_sent_balance_hit"
	statusErrTotalBalanceHit         = "err_total_balance_hit"
)

type messageIn struct {
	To   string `binding:"required" json:"to"`
	Text string `binding:"required" json:"text"`
}

type sendRequest struct {
	IdentityID int64       `binding:"required"      json:"identity_id"`
	Express    bool        `json:"express"`
	Messages   []messageIn `binding:"required,min=1" json:"messages"`
}

type sendResult struct {
	Status   string `json:"status"`
	Accepted int    `json:"accepted"`
	Rejected int    `json:"rejected"`
	Total    int    `json:"total"`
}

type pingResponse struct {
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// admissionMsg is one message within an admissionRequest.
type admissionMsg struct {
	to   string
	text string
}

// admissionRequest is one POST call queued for the admission batcher. resultCh receives exactly one sendResult once
// its batch has been locked (or failed to lock).
type admissionRequest struct {
	identityID int64
	express    bool
	messages   []admissionMsg
	receivedAt time.Time
	resultCh   chan sendResult
}

// dispatchItem is one accepted, paid message waiting to be sent to the operator. hourBucket + cost identify which
// balance_locks row to release once the operator confirms delivery.
type dispatchItem struct {
	identityID int64
	hourBucket time.Time
	to         string
	text       string
	cost       int64
	receivedAt time.Time
}
