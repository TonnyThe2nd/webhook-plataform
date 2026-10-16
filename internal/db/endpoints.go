package db

import (
	"context"
	"fmt"

	"github.com/TonnyThe2nd/webhook-platform/internal/domain"
)

func (s *Store) EndpointsSubscribedTo(ctx context.Context, tenantID, eventType string) ([]domain.Endpoint, error) {
	const q = `
		SELECT id, tenant_id, url,secret, event_types, active, created_at
		FROM endpoints
		WHERE tenant_id = $1 AND active = true AND ($2 = ANY(event_types) OR event_types = '{}')
        ORDER BY created_at
	`

	rows, err := s.pool.Query(ctx, q, tenantID, eventType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var endpoints []domain.Endpoint
	for rows.Next() {
		var e domain.Endpoint
		if err := rows.Scan(
			&e.ID, &e.TenantID, &e.URL, &e.Secret,
			&e.EventTypes, &e.Active, &e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("lendo endpoint: %w", err)
		}
		endpoints = append(endpoints, e)
	}
	return endpoints, rows.Err()
}
