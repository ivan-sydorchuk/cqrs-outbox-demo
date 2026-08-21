import 'dotenv/config';
import { PrismaClient } from '@prisma/client';
import { randomUUID } from 'crypto';

/**
 * Proves, against a real Postgres instance, exactly why a cursor based on
 * "position" (the BIGSERIAL primary key) alone is unsafe for the outbox
 * poller — before we build the worker that avoids it. See mentor note
 * "Every 'Just Poll the Table' Hides Three Traps": build the naive version
 * first, watch it bite, then fix it.
 *
 * The mechanism: BIGSERIAL hands out the next number when an INSERT
 * *statement* runs, not when its transaction commits. Two concurrent
 * transactions can therefore commit in the opposite order to the position
 * values they were assigned. Under READ COMMITTED, a still-open transaction's
 * row is invisible to everyone else until it commits — so a poller can see
 * a HIGHER position (from a transaction that started later but committed
 * first) while a LOWER position (still uncommitted) doesn't exist yet from
 * its point of view. If the poller advances its cursor to that higher
 * position, the lower one is skipped forever once it finally does commit,
 * because "position > cursor" now excludes it permanently.
 */

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function insertOrder(prisma: PrismaClient, orderId: string, amount: number) {
  await prisma.order.create({
    data: {
      orderId,
      customerId: randomUUID(),
      amount,
      status: 'Pending',
    },
  });
}

async function main() {
  const prisma = new PrismaClient();
  const orderA = randomUUID();
  const orderB = randomUUID();

  console.log('Opening SLOW transaction A: insert order + outbox row, then hold before committing...');
  const slowTx = prisma.$transaction(
    async (tx) => {
      await tx.order.create({ data: { orderId: orderA, customerId: randomUUID(), amount: 10, status: 'Pending' } });
      const [row] = await tx.$queryRaw<{ position: bigint }[]>`
        INSERT INTO outbox (aggregate_id, event_type, payload)
        VALUES (${orderA}::uuid, 'OrderPlacedEvent', ${JSON.stringify({ aggregateId: orderA })}::jsonb)
        RETURNING position
      `;
      console.log(`  [A] inserted outbox position=${row.position}, holding the transaction open for 4s...`);
      await sleep(4000);
      console.log('  [A] committing now.');
    },
    { timeout: 15_000 },
  );

  // Give A time to insert and start waiting before B starts, so B is
  // guaranteed to run — and commit — entirely inside A's open window.
  await sleep(500);

  console.log('\nRunning FAST transaction B (separate, autocommits immediately)...');
  await insertOrder(prisma, orderB, 20);
  const [rowB] = await prisma.$queryRaw<{ position: bigint }[]>`
    INSERT INTO outbox (aggregate_id, event_type, payload)
    VALUES (${orderB}::uuid, 'OrderPlacedEvent', ${JSON.stringify({ aggregateId: orderB })}::jsonb)
    RETURNING position
  `;
  console.log(`  [B] inserted and committed outbox position=${rowB.position}.`);

  console.log('\nNAIVE poll — "SELECT ... ORDER BY position" — while A is still uncommitted:');
  const naiveRows = await prisma.$queryRaw<{ position: bigint; aggregate_id: string }[]>`
    SELECT position, aggregate_id FROM outbox ORDER BY position
  `;
  for (const row of naiveRows) {
    console.log(`  position=${row.position} aggregate_id=${row.aggregate_id}`);
  }
  const maxSeen = naiveRows[naiveRows.length - 1]?.position;
  console.log(
    `  A naive worker sees position ${maxSeen} as the newest row (B's — A's is still invisible) and would ` +
      `advance its cursor there.`,
  );

  await slowTx;
  console.log('\nTransaction A has now committed. Its outbox row exists in the table — but:');
  const afterCommit = await prisma.$queryRaw<{ position: bigint }[]>`
    SELECT position FROM outbox WHERE aggregate_id = ${orderA}::uuid
  `;
  console.log(`  A's row is at position=${afterCommit[0]?.position}, which is <= the cursor (${maxSeen}) already advanced past.`);
  console.log('  A "WHERE position > cursor" poller will NEVER select this row again. Event permanently lost.');
  console.log(
    '\nThis is exactly why the real worker (src/worker) orders/filters by (transaction_id, position) and only\n' +
      'reads transactions already below pg_snapshot_xmin() — see src/worker/outbox-reader.ts.',
  );

  await prisma.$disconnect();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
