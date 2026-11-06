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

-- Block 10 additions — outbox_nest only. See block10-worker-extension.sql
-- for why (transaction_id ordering, the event_subscription checkpoint).
ALTER TABLE outbox
    ADD COLUMN transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id();

CREATE INDEX idx_outbox_transaction_id ON outbox (transaction_id, position);

CREATE TABLE event_subscription (
    name                 TEXT PRIMARY KEY,
    last_transaction_id  xid8   NOT NULL DEFAULT '0',
    last_position        BIGINT NOT NULL DEFAULT 0
);

INSERT INTO event_subscription (name) VALUES ('kafka-order-events-0'), ('kafka-order-events-1');

-- Block 11: a separate database for the event-sourcing subproject
-- (event-sourcing/). This is a genuine event store, not an outbox -- there is
-- no `orders` row that holds current state; `order_events` is the only
-- source of truth and `order_projection` is a read model rebuilt from it.
CREATE DATABASE event_sourcing;

\connect event_sourcing

-- Append-only. One row per domain event, (stream_id, version) is the
-- optimistic-concurrency key: an INSERT that collides on it means someone
-- else already appended at that version, so the application must reject the
-- command instead of silently overwriting history.
CREATE TABLE order_events (
    stream_id    UUID NOT NULL,
    version      INTEGER NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_id, version)
);

-- Bonus: a snapshot every N events so `load()` doesn't have to replay the
-- whole stream from version 1. Always optional -- deleting every row here
-- must not change what `load()` returns, only how fast it returns it.
CREATE TABLE order_snapshots (
    stream_id   UUID PRIMARY KEY,
    version     INTEGER NOT NULL,
    state       JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Bonus: a projection (read model). Updated by replaying/folding
-- order_events, never written to directly by a command handler. Eventually
-- consistent with the event log by construction.
CREATE TABLE order_projection (
    order_id    UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    status      TEXT NOT NULL,
    version     INTEGER NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);
