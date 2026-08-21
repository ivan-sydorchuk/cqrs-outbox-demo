-- Block 10 additions. Applied ONLY to outbox_nest — Block 10 scopes the
-- worker/relay to "your CQRS TypeScript app from Block 9", so outbox_dotnet
-- is intentionally left on the plain Block 9 schema (sql/schema.sql).
--
-- Run against outbox_nest, after schema.sql, e.g.:
--   psql -U demo -d outbox_nest -f block10-worker-extension.sql

-- Tags each outbox row with the id of the transaction that inserted it.
-- WHY this exists (not just BIGSERIAL "position"): Postgres assigns
-- BIGSERIAL values when a statement *starts*, but transactions can commit
-- out of order (a later-starting, faster transaction can commit before an
-- earlier one that's still running). A cursor based on "position" alone can
-- read position 105 committed, advance the checkpoint past it, and then
-- never come back for position 104 once it finally commits — a silently
-- lost event. transaction_id (Postgres type xid8, strictly increases with
-- COMMIT order) fixes this: the worker only reads rows whose transaction_id
-- is already below the oldest transaction still in flight
-- (pg_snapshot_xmin(pg_current_snapshot())), so it never observes a "gap"
-- that a slower, still-committing transaction might still fill.
ALTER TABLE outbox
    ADD COLUMN transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id();

-- DEFAULT above means ordinary INSERTs (including Prisma's, which never
-- lists this column explicitly since it's mapped as Unsupported("xid8"))
-- get the correct value for free, evaluated in the inserting transaction.

CREATE INDEX idx_outbox_transaction_id ON outbox (transaction_id, position);

-- One row per logical consumer of the outbox ("subscription" in the
-- reference repo's vocabulary). The worker claims this row with
-- SELECT ... FOR UPDATE SKIP LOCKED before reading a batch, so two worker
-- instances polling the same subscription never grab the same rows.
-- last_transaction_id defaults to '0': Postgres's real transaction ids start
-- at 3 (FirstNormalTransactionId), so '0' correctly means "nothing consumed
-- yet" without needing a nullable column and null-checks everywhere.
CREATE TABLE event_subscription (
    name                TEXT PRIMARY KEY,
    last_transaction_id xid8   NOT NULL DEFAULT '0',
    last_position        BIGINT NOT NULL DEFAULT 0
);

-- Two shards = two independently claimable units of work, which is what
-- lets two worker instances actually run in parallel rather than one
-- active + one idle standby. An outbox row belongs to shard
-- abs(hashtext(aggregate_id::text)) % 2 — see src/worker/outbox-relay.ts.
-- Changing shard count later means picking up unprocessed rows under the
-- new hash, so treat it as a one-time choice for this demo, not something
-- to resize casually.
INSERT INTO event_subscription (name) VALUES ('kafka-order-events-0'), ('kafka-order-events-1');
