package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/TonnyThe2nd/webhook-platform/internal/domain"
	"github.com/jackc/pgx/v5"
)

func (s *Store) InsertDelivery(ctx context.Context, eventID, endpointID, tenantID string) (*domain.Delivery, error) {
	const q = `
        INSERT INTO deliveries (event_id, endpoint_id, tenant_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (event_id, endpoint_id) DO NOTHING
        RETURNING id, status, attempts, next_attempt_at, created_at
    `
	var delivery domain.Delivery
	err := s.pool.QueryRow(ctx, q, eventID, endpointID, tenantID).Scan(
		&delivery.ID, &delivery.Status, &delivery.Attempts,
		&delivery.NextAttemptAt, &delivery.CreatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		existing, err := s.getDeliveryByEventAndEndpoint(ctx, eventID, endpointID)
		if err != nil {
			return nil, fmt.Errorf("obtendo entrega existente: %w", err)
		}
		return existing, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inserindo entrega: %w", err)
	}
	delivery.EventID = eventID
	delivery.EndpointID = endpointID
	delivery.TenantID = tenantID
	return &delivery, nil
}

func (s *Store) getDeliveryByEventAndEndpoint(ctx context.Context, eventID, endpointID string) (*domain.Delivery, error) {
	const q = `
		SELECT id, event_id, endpoint_id, tenant_id, status, attempts, next_attempt_at, created_at
		FROM deliveries
		WHERE event_id = $1 AND endpoint_id = $2
	`
	var delivery domain.Delivery
	err := s.pool.QueryRow(ctx, q, eventID, endpointID).Scan(
		&delivery.ID, &delivery.EventID, &delivery.EndpointID,
		&delivery.TenantID, &delivery.Status, &delivery.Attempts,
		&delivery.NextAttemptAt, &delivery.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("lendo entrega: %w", err)
	}
	return &delivery, nil
}
