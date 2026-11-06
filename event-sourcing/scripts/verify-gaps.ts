import 'dotenv/config';
import 'reflect-metadata';
import { randomUUID } from 'crypto';
import { NestFactory } from '@nestjs/core';
import { Pool } from 'pg';
import { AppModule } from '../src/app.module';
import { Order } from '../src/domain/order.aggregate';
import { OrderRepository } from '../src/order.repository';

/**
 * Two things `src/demo.ts` never actually exercised:
 *
 * 1. A *genuine* concurrent race -- two `save()` calls that really overlap
 *    (via `Promise.all`), not two sequential calls where the second is just
 *    stale. Proves the Postgres `SELECT ... FOR UPDATE` and KurrentDB's
 *    native expected-revision check both serialize correctly instead of
 *    both "winning" under real parallelism.
 * 2. The snapshot write/read path -- `demo.ts` never raises enough events
 *    on one stream to cross `SNAPSHOT_EVERY`, so `PostgresSnapshotStore`
 *    was never actually written to or read from. This forces it with
 *    `SNAPSHOT_EVERY=1` and proves `load()` returns the identical aggregate
 *    whether or not the snapshot row exists.
 */
const storeName = process.env.EVENT_STORE === 'kurrentdb' ? 'KurrentDB' : 'Postgres';

async function main() {
  console.log(`=== Gap verification — EVENT_STORE=${storeName} ===\n`);

  console.log('1) Genuine concurrent race: two stale copies save() at the same instant');
  const app = await NestFactory.createApplicationContext(AppModule);
  const repository = app.get(OrderRepository);
  const customerId = randomUUID();

  const raceId = randomUUID();
  await repository.save(Order.place(raceId, customerId, 15));
  const copyA = await repository.load(raceId);
  const copyB = await repository.load(raceId);
  copyA!.pay();
  copyB!.cancel('racing');

  const [resultA, resultB] = await Promise.allSettled([repository.save(copyA!), repository.save(copyB!)]);
  const outcomes = [resultA, resultB].map((r) => (r.status === 'fulfilled' ? 'OK' : r.reason.constructor.name));
  console.log(`   outcomes: A=${outcomes[0]} B=${outcomes[1]}`);
  if (outcomes.filter((o) => o === 'OK').length !== 1) {
    throw new Error(`Expected exactly one writer to win a real race, got: ${outcomes.join(', ')}`);
  }
  console.log('   -> exactly one writer won, the other got ConcurrencyError. Real race, not just staleness.\n');

  console.log('2) Snapshot path: SNAPSHOT_EVERY=1 forces a snapshot write on every save()');
  process.env.SNAPSHOT_EVERY = '1';
  const snapAppApp = await NestFactory.createApplicationContext(AppModule);
  const snapRepo = snapAppApp.get(OrderRepository);

  const snapId = randomUUID();
  await snapRepo.save(Order.place(snapId, customerId, 8));
  const loaded1 = await snapRepo.load(snapId);
  loaded1!.pay();
  await snapRepo.save(loaded1!);

  const adminPool = new Pool({ connectionString: process.env.EVENT_SOURCING_DATABASE_URL });
  const { rows: before } = await adminPool.query('SELECT version, state FROM order_snapshots WHERE stream_id = $1', [
    snapId,
  ]);
  console.log(`   order_snapshots row after 2 events: ${before.length ? JSON.stringify(before[0]) : '(none!)'}`);
  if (before.length === 0) throw new Error('Expected a snapshot row to exist with SNAPSHOT_EVERY=1');

  const withSnapshot = await snapRepo.load(snapId);
  await adminPool.query('DELETE FROM order_snapshots WHERE stream_id = $1', [snapId]);
  const withoutSnapshot = await snapRepo.load(snapId);
  const identical =
    withSnapshot!.status === withoutSnapshot!.status &&
    withSnapshot!.version === withoutSnapshot!.version &&
    withSnapshot!.amount === withoutSnapshot!.amount;
  console.log(
    `   with snapshot: status=${withSnapshot!.status} version=${withSnapshot!.version} | ` +
      `without (deleted): status=${withoutSnapshot!.status} version=${withoutSnapshot!.version}`,
  );
  if (!identical) throw new Error('Deleting the snapshot changed what load() returned -- it must not.');
  console.log('   -> identical result with or without the snapshot row. Cache, not a second source of truth.\n');

  await adminPool.end();
  await app.close();
  await snapAppApp.close();
  console.log('All gap checks passed.');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
