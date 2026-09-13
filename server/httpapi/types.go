package httpapi

import "time"

type messageIn struct {
	To   string `binding:"required" json:"to"`
	Text string `binding:"required" json:"text"`
}

type sendRequest struct {
	IdentityID int64       `binding:"required"      json:"identity_id"`
	Express    bool        `json:"express"`
	Messages   []messageIn `binding:"required,min=1" json:"messages"`
}

type pingResponse struct {
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

type errorResponse struct {
	Error string `json:"error"`
}
