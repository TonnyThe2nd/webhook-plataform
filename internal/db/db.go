package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ConnectDB(ctx context.Context, url string) (*pgxpool.Pool, error) {

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("erro ao parsear URL: %w", err)
	}

	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar pool de conexões: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("erro ao pingar o banco de dados: %w", err)
	}

	return pool, nil
}
