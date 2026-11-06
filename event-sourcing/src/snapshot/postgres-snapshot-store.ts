import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool } from 'pg';
import { OrderSnapshot } from '../domain/order.aggregate';
import { SnapshotStore } from './snapshot-store';

/**
 * `order_snapshots` is the one table in this subproject that gets UPDATEd
 * (via upsert) -- deliberately, since a snapshot is a replaceable cache of
 * `apply` folded so far, not part of the append-only log.
 */
@Injectable()
export class PostgresSnapshotStore implements SnapshotStore, OnModuleDestroy {
  private readonly pool = new Pool({ connectionString: process.env.EVENT_SOURCING_DATABASE_URL });

  async load(streamId: string): Promise<OrderSnapshot | null> {
    const { rows } = await this.pool.query<{ state: OrderSnapshot }>(
      `SELECT state FROM order_snapshots WHERE stream_id = $1`,
      [streamId],
    );
    return rows[0]?.state ?? null;
  }

  async save(snapshot: OrderSnapshot): Promise<void> {
    await this.pool.query(
      `INSERT INTO order_snapshots (stream_id, version, state, occurred_at)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (stream_id) DO UPDATE SET version = $2, state = $3, occurred_at = now()`,
      [snapshot.orderId, snapshot.version, JSON.stringify(snapshot)],
    );
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }
}
