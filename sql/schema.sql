-- Canonical write-model + outbox schema.
-- This exact DDL is applied verbatim to BOTH databases (outbox_dotnet, outbox_nest)
-- by sql/init-databases.sql, so the two stacks are provably targeting the same
-- table shapes rather than two ORMs independently re-deriving "similar" schemas.

CREATE TABLE orders (
    order_id     UUID PRIMARY KEY,
    customer_id  UUID NOT NULL,
    amount       NUMERIC(12, 2) NOT NULL,
    status       TEXT NOT NULL,
    version      INTEGER NOT NULL DEFAULT 1,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only. Never UPDATE or DELETE a row here.
CREATE TABLE outbox (
    position     BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_aggregate_id ON outbox (aggregate_id);
