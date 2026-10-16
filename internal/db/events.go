package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/TonnyThe2nd/webhook-platform/internal/domain"
	"github.com/jackc/pgx/v5"
)

var ErrEventNotFound = errors.New("event not found")

func (s *Store) InsertEvent(ctx context.Context, event *domain.Event) (*domain.Event, bool, error) {

	const q = `
		INSERT INTO events (tenant_id, type, payload, idempotency_key)
		VALUES ($1, $2, $3, NULLIF($4, ''))
		ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING id, created_at
	`

	var created domain.Event

	err := s.pool.QueryRow(ctx, q, event.TenantID, event.Type, event.Payload, event.IdempotencyKey).Scan(&created.ID, &created.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		existing, err := s.getEventByIdempotencyKey(ctx, event.TenantID, event.IdempotencyKey)
		if err != nil {
			return nil, false, err
		}
		return existing, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("inserindo evento: %w", err)
	}
	created.TenantID = event.TenantID
	created.Type = event.Type
	created.Payload = event.Payload
	created.IdempotencyKey = event.IdempotencyKey

	return &created, true, nil
}

func (s *Store) getEventByIdempotencyKey(ctx context.Context, tenantID, idempotencyKey string) (*domain.Event, error) {
	const q = `
		SELECT id, tenant_id, type, payload, idempotency_key, created_at
		FROM events
		WHERE tenant_id = $1 AND idempotency_key = $2
	`

	var event domain.Event
	err := s.pool.QueryRow(ctx, q, tenantID, idempotencyKey).Scan(&event.ID, &event.TenantID, &event.Type, &event.Payload, &event.IdempotencyKey, &event.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &event, nil
}
