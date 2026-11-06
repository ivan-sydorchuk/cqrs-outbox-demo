import 'dotenv/config';
import { Client } from 'pg';

/**
 * sql/init-databases.sql (repo root) already creates `event_sourcing` and
 * its tables -- but only the first time the postgres container's data
 * volume is created (docker-entrypoint-initdb.d convention). If your
 * `cqrs-outbox-postgres` container already existed before Block 11 (i.e.
 * dotnet/ or nest/ were ever run), that script will not re-run, so this
 * does the same thing idempotently against the live container instead of
 * requiring you to destroy it.
 */
async function main() {
  const admin = new Client({ connectionString: process.env.EVENT_SOURCING_ADMIN_DATABASE_URL });
  await admin.connect();
  const { rowCount } = await admin.query(`SELECT 1 FROM pg_database WHERE datname = 'event_sourcing'`);
  if (!rowCount) {
    console.log('Creating database event_sourcing...');
    await admin.query('CREATE DATABASE event_sourcing');
  } else {
    console.log('Database event_sourcing already exists.');
  }
  await admin.end();

  const db = new Client({ connectionString: process.env.EVENT_SOURCING_DATABASE_URL });
  await db.connect();
  await db.query(`
    CREATE TABLE IF NOT EXISTS order_events (
      stream_id    UUID NOT NULL,
      version      INTEGER NOT NULL,
      event_type   TEXT NOT NULL,
      payload      JSONB NOT NULL,
      occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (stream_id, version)
    );
    CREATE TABLE IF NOT EXISTS order_snapshots (
      stream_id   UUID PRIMARY KEY,
      version     INTEGER NOT NULL,
      state       JSONB NOT NULL,
      occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS order_projection (
      order_id    UUID PRIMARY KEY,
      customer_id UUID NOT NULL,
      amount      NUMERIC(12, 2) NOT NULL,
      status      TEXT NOT NULL,
      version     INTEGER NOT NULL,
      updated_at  TIMESTAMPTZ NOT NULL
    );
  `);
  console.log('Tables ready.');
  await db.end();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
