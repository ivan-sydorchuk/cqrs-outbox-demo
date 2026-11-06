import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool } from 'pg';
import { ConcurrencyError, EventStore, NewEvent, ReadStreamOptions, StoredEvent } from './event-store';

const UNIQUE_VIOLATION = '23505';

/**
 * `order_events` (see sql/init-databases.sql, database `event_sourcing`):
 * append-only, `PRIMARY KEY (stream_id, version)`. Every write here is a
 * plain INSERT -- there is no UPDATE/DELETE anywhere in this class, which is
 * the whole "append-only means append-only" rule made structurally true
 * rather than just promised.
 */
@Injectable()
export class PostgresEventStore implements EventStore, OnModuleDestroy {
  private readonly pool = new Pool({ connectionString: process.env.EVENT_SOURCING_DATABASE_URL });

  async append(streamId: string, expectedVersion: number, events: NewEvent[]): Promise<void> {
    if (events.length === 0) return;

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      // Locks the stream's last row (if any) so a concurrent appender blocks
      // here rather than racing past this check. Postgres rejects `FOR
      // UPDATE` combined with an aggregate directly, hence ORDER BY ...
      // LIMIT 1 instead of MAX() -- equivalent since rows are only ever
      // appended with strictly increasing version. The "brand new stream"
      // case (no rows to lock) is still covered below by the
      // unique-constraint catch.
      const { rows } = await client.query<{ version: number }>(
        `SELECT version FROM order_events WHERE stream_id = $1 ORDER BY version DESC LIMIT 1 FOR UPDATE`,
        [streamId],
      );
      const actualVersion = rows[0] ? Number(rows[0].version) : 0;
      if (actualVersion !== expectedVersion) {
        await client.query('ROLLBACK');
        throw new ConcurrencyError(streamId, expectedVersion, actualVersion);
      }

      let version = expectedVersion;
      for (const event of events) {
        version += 1;
        await client.query(
          `INSERT INTO order_events (stream_id, version, event_type, payload, occurred_at)
           VALUES ($1, $2, $3, $4, $5)`,
          [streamId, version, event.eventType, JSON.stringify(event.payload), event.occurredAt],
        );
      }

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      if (isUniqueViolation(error)) {
        throw new ConcurrencyError(streamId, expectedVersion);
      }
      throw error;
    } finally {
      client.release();
    }
  }

  async readStream(streamId: string, options: ReadStreamOptions = {}): Promise<StoredEvent[]> {
    const { fromVersion, toVersion } = options;
    const conditions = ['stream_id = $1'];
    const params: unknown[] = [streamId];
    if (fromVersion !== undefined) {
      params.push(fromVersion);
      conditions.push(`version > $${params.length}`);
    }
    if (toVersion !== undefined) {
      params.push(toVersion);
      conditions.push(`version <= $${params.length}`);
    }

    const { rows } = await this.pool.query<{
      stream_id: string;
      version: number;
      event_type: string;
      payload: Record<string, unknown>;
      occurred_at: Date;
    }>(
      `SELECT stream_id, version, event_type, payload, occurred_at
       FROM order_events WHERE ${conditions.join(' AND ')} ORDER BY version ASC`,
      params,
    );

    return rows.map((row) => ({
      streamId: row.stream_id,
      version: row.version,
      eventType: row.event_type,
      payload: row.payload,
      occurredAt: row.occurred_at,
    }));
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }
}

function isUniqueViolation(error: unknown): boolean {
  return typeof error === 'object' && error !== null && (error as { code?: string }).code === UNIQUE_VIOLATION;
}
