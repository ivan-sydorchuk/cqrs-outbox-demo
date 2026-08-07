-- Runs once when the postgres container's data volume is first created
-- (docker-entrypoint-initdb.d convention). Creates two databases — one per
-- stack — and applies the identical canonical schema (see schema.sql) to each,
-- so both implementations are provably working against the same table shapes
-- without two different migration engines racing to own one physical schema.

CREATE DATABASE outbox_dotnet;
CREATE DATABASE outbox_nest;

\connect outbox_dotnet

CREATE TABLE orders (
    order_id     UUID PRIMARY KEY,
    customer_id  UUID NOT NULL,
    amount       NUMERIC(12, 2) NOT NULL,
    status       TEXT NOT NULL,
    version      INTEGER NOT NULL DEFAULT 1,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    position     BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_aggregate_id ON outbox (aggregate_id);

\connect outbox_nest

CREATE TABLE orders (
    order_id     UUID PRIMARY KEY,
    customer_id  UUID NOT NULL,
    amount       NUMERIC(12, 2) NOT NULL,
    status       TEXT NOT NULL,
    version      INTEGER NOT NULL DEFAULT 1,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    position     BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_aggregate_id ON outbox (aggregate_id);
