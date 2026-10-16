package domain

import "time"

type Endpoint struct {
	ID         string
	TenantID   string
	URL        string
	Secret     string
	EventTypes []string
	Active     bool
	CreatedAt  time.Time
}
