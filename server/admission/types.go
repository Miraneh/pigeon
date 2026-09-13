package admission

import "time"

const (
	StatusOK                         = "ok"
	StatusErrPartiallySentBalanceHit = "err_partially_sent_balance_hit"
	StatusErrTotalBalanceHit         = "err_total_balance_hit"
)

type Result struct {
	Status   string `json:"status"`
	Accepted int    `json:"accepted"`
	Rejected int    `json:"rejected"`
	Total    int    `json:"total"`
}

// Message is one message within a Request.
type Message struct {
	To   string
	Text string
}

// Request is one POST call queued for the admission batcher. ResultCh receives exactly one Result once its batch has
// been locked (or failed to lock).
type Request struct {
	IdentityID int64
	Express    bool
	Messages   []Message
	ReceivedAt time.Time
	ResultCh   chan Result
}
