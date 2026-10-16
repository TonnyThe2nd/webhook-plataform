package domain

import (
	"encoding/json"
	"time"
)

type Event struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}
