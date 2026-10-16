package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TonnyThe2nd/webhook-platform/internal/domain"
	"github.com/jackc/pgx/v5"
)

var ErrDeliveryNotFound = errors.New("delivery not found")

func (s *Store) GetDelivery(ctx context.Context, id string) (*domain.Delivery, error) {
	const q = `
		SELECT d.id, d.event_id, d.endpoint_id, d.tenant_id, d.status,
		       d.attempts, d.next_attempt_at, COALESCE(d.last_status_code, 0),
		       COALESCE(d.last_error, ''), d.delivered_at, d.created_at,
		       ep.url, ep.secret, e.type, e.payload
		FROM deliveries d
		JOIN events e ON e.id = d.event_id AND e.tenant_id = d.tenant_id
		JOIN endpoints ep ON ep.id = d.endpoint_id AND ep.tenant_id = d.tenant_id
		WHERE d.id = $1
	`
	var d domain.Delivery
	err := s.pool.QueryRow(ctx, q, id).Scan(
		&d.ID, &d.EventID, &d.EndpointID, &d.TenantID, &d.Status,
		&d.Attempts, &d.NextAttemptAt, &d.LastStatusCode, &d.LastError,
		&d.DeliveredAt, &d.CreatedAt, &d.URL, &d.Secret, &d.EventType, &d.Payload,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrDeliveryNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lendo entrega: %w", err)
	}
	return &d, nil
}

func (s *Store) MarkDelivered(ctx context.Context, id string, statusCode int, latencyMs int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries SET status = 'delivered', delivered_at = now(),
		last_status_code = $2, last_error = NULL WHERE id = $1
	`, id, statusCode)
	return err
}

func (s *Store) MarkFailed(ctx context.Context, id string, statusCode int, errMsg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries SET status = 'failed', last_status_code = $2,
		last_error = NULLIF($3, '') WHERE id = $1
	`, id, statusCode, errMsg)
	return err
}

func (s *Store) Reschedule(ctx context.Context, id string, attempt int, next time.Time, statusCode int, errMsg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries SET status = 'pending', attempts = GREATEST(attempts, $2),
		next_attempt_at = $3, last_status_code = $4, last_error = NULLIF($5, '')
		WHERE id = $1
	`, id, attempt, next, statusCode, errMsg)
	return err
}

func (s *Store) RecordAttempt(ctx context.Context, deliveryID string, attemptNo, statusCode, latencyMs int, errMsg string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
		INSERT INTO attempts (delivery_id, attempt_no, status_code, latency_ms, error)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''))
	`, deliveryID, attemptNo, statusCode, latencyMs, errMsg)
	if err != nil {
		return fmt.Errorf("registrando tentativa: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE deliveries SET attempts = GREATEST(attempts, $2) WHERE id = $1`, deliveryID, attemptNo)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
