import { PrismaClient } from '@prisma/client';
import { Producer } from 'kafkajs';
import { workerConfig } from './config';

interface SubscriptionRow {
  name: string;
  last_transaction_id: string;
  last_position: bigint;
}

interface OutboxRow {
  position: bigint;
  aggregate_id: string;
  event_type: string;
  payload: unknown;
  occurred_at: Date;
  transaction_id: string;
}

export interface PollResult {
  /** false = someone else already holds this shard's lock right now; nothing was done. */
  claimed: boolean;
  published: number;
}

/**
 * One poll cycle for one shard. This is the whole mechanism from the block's
 * reading list, assembled:
 *
 *  1. SELECT ... FOR UPDATE SKIP LOCKED on the shard's event_subscription
 *     row. If another worker instance already holds it, this returns zero
 *     rows immediately (no blocking) — the caller just tries again next
 *     tick. This is what makes horizontal scaling safe: two instances can
 *     never process the same shard at the same time.
 *  2. Read a batch ordered by (transaction_id, position), filtered to
 *     transaction_id < pg_snapshot_xmin(pg_current_snapshot()) — i.e.
 *     strictly below the oldest transaction still in flight anywhere in the
 *     database. This is what makes the ordering-bug demo (see
 *     scripts/demo-ordering-bug.ts) impossible here: we never read past a
 *     position whose transaction might still be uncommitted, so the cursor
 *     never advances beyond a gap that could still be filled in.
 *  3. Publish the batch to Kafka, keyed by aggregate_id (same aggregate =
 *     same partition = preserved per-aggregate order).
 *  4. Only AFTER Kafka acknowledges the send, advance the checkpoint
 *     (last_transaction_id, last_position) to the batch's last row.
 *
 * Steps 1-4 run inside one Prisma interactive transaction, so the shard's
 * row lock is held for the whole cycle including the Kafka round trip.
 * That's a deliberate, documented tradeoff (see README "Design notes"): it
 * keeps the claim/read/publish/checkpoint sequence trivially consistent at
 * the cost of holding one row lock for a network call's duration, which is
 * fine at this volume and gets swapped for a lease-based claim if it ever
 * isn't.
 *
 * Crash safety: if the process dies at any point before the checkpoint
 * UPDATE commits — including "died right after Kafka acked" — the whole
 * transaction never commits, so on restart the same batch is read and
 * published again. That's at-least-once, and it's why Kafka consumers of
 * this topic must dedupe by `position` (sent as the event-id header).
 */
export async function pollShard(prisma: PrismaClient, producer: Producer, shardIndex: number): Promise<PollResult> {
  const name = `kafka-order-events-${shardIndex}`;

  return prisma.$transaction(
    async (tx) => {
      // xid8 columns are cast to ::text in the SELECT list — Prisma's raw
      // query deserializer has no mapping for xid8 and errors otherwise
      // ("Failed to deserialize column of type 'xid8'"). Comparisons/casts
      // back to xid8 still happen in SQL, so this loses no precision.
      const claimed = await tx.$queryRaw<SubscriptionRow[]>`
        SELECT name, last_transaction_id::text AS last_transaction_id, last_position
        FROM event_subscription
        WHERE name = ${name}
        FOR UPDATE SKIP LOCKED
      `;

      if (claimed.length === 0) {
        return { claimed: false, published: 0 };
      }

      const { last_transaction_id: lastTx, last_position: lastPos } = claimed[0];

      const rows = await tx.$queryRaw<OutboxRow[]>`
        SELECT position, aggregate_id, event_type, payload, occurred_at, transaction_id::text AS transaction_id
        FROM outbox
        WHERE abs(hashtext(aggregate_id::text)) % ${workerConfig.shardCount} = ${shardIndex}
          AND (transaction_id, position) > (${lastTx}::xid8, ${lastPos}::bigint)
          AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
        ORDER BY transaction_id, position
        LIMIT ${workerConfig.batchSize}
      `;

      if (rows.length === 0) {
        return { claimed: true, published: 0 };
      }

      await producer.send({
        topic: workerConfig.kafkaTopic,
        messages: rows.map((row) => ({
          key: row.aggregate_id,
          value: JSON.stringify({
            position: row.position.toString(),
            aggregateId: row.aggregate_id,
            eventType: row.event_type,
            payload: row.payload,
            occurredAt: row.occurred_at,
          }),
          headers: { 'event-id': row.position.toString() },
        })),
      });

      const last = rows[rows.length - 1];
      await tx.$executeRaw`
        UPDATE event_subscription
        SET last_transaction_id = ${last.transaction_id}::xid8, last_position = ${last.position}
        WHERE name = ${name}
      `;

      return { claimed: true, published: rows.length };
    },
    { timeout: 15_000, maxWait: 5_000 },
  );
}
