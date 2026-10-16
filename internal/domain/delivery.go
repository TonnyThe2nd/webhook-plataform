package domain

import (
	"encoding/json"
	"time"
)

type DeliveryStatus string

const (
	DeliveryPending   DeliveryStatus = "pending"
	DeliveryDelivered DeliveryStatus = "delivered"
	DeliveryFailed    DeliveryStatus = "failed"
)

type Delivery struct {
	ID             string
	EventID        string
	EndpointID     string
	TenantID       string
	Status         DeliveryStatus
	Attempts       int
	NextAttemptAt  time.Time
	LastStatusCode int
	LastError      string
	DeliveredAt    *time.Time
	CreatedAt      time.Time
	URL            string `json:"-"`
	Secret         string `json:"-"`
	EventType      string
	Payload        json.RawMessage
}
