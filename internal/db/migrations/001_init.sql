CREATE TABLE tenants (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE endpoints (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    url                 TEXT NOT NULL,
    secret              TEXT NOT NULL,
    event_types         TEXT[] NOT NULL DEFAULT '{}',
    active              BOOLEAN NOT NULL DEFAULT true,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX endpoints_tenant_idx ON endpoints (tenant_id);

CREATE TABLE events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    type            TEXT NOT NULL,
    payload         JSONB NOT NULL,
    idempotency_key TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX events_tenant_idem_uniq
    ON events (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE deliveries (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id          UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    endpoint_id       UUID NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    tenant_id         UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    status            TEXT NOT NULL DEFAULT 'pending',
    attempts          INT NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status_code  INT,
    last_error        TEXT,
    delivered_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX deliveries_event_endpoint_uniq
    ON deliveries (event_id, endpoint_id);

CREATE INDEX deliveries_pending_idx
    ON deliveries (next_attempt_at)
    WHERE status = 'pending';

CREATE TABLE attempts (
    id           BIGSERIAL PRIMARY KEY,
    delivery_id  UUID NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt_no   INT NOT NULL,
    status_code  INT,
    latency_ms   INT,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX attempts_delivery_idx ON attempts (delivery_id);