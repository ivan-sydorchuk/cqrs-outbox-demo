import 'dotenv/config';
import 'reflect-metadata';
import { randomUUID } from 'crypto';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { Order } from './domain/order.aggregate';
import { OrderRepository } from './order.repository';
import { OrderSummaryProjection } from './projection/order-summary.projection';

const storeName = process.env.EVENT_STORE === 'kurrentdb' ? 'KurrentDB' : 'Postgres';

async function main() {
  console.log(`=== Event-sourced Order demo — EVENT_STORE=${storeName} ===\n`);

  const app = await NestFactory.createApplicationContext(AppModule);
  const repository = app.get(OrderRepository);
  const projection = app.get(OrderSummaryProjection);

  const orderId = randomUUID();
  const customerId = randomUUID();

  console.log(`1) Place order ${orderId}`);
  await repository.save(Order.place(orderId, customerId, 42.5));

  console.log('2) Reload purely by replaying its events (no "current row" read anywhere)');
  const reloaded = await repository.load(orderId);
  console.log(`   -> status=${reloaded!.status} version=${reloaded!.version} amount=${reloaded!.amount}\n`);

  console.log('3) Pay the order, reload again');
  reloaded!.pay();
  await repository.save(reloaded!);
  const afterPay = await repository.load(orderId);
  console.log(`   -> status=${afterPay!.status} version=${afterPay!.version}\n`);

  console.log('4) Temporal query: state as of version 1 (before it was paid)');
  const asOfV1 = await repository.load(orderId, 1);
  console.log(`   -> status=${asOfV1!.status} version=${asOfV1!.version}\n`);

  console.log('5) Optimistic concurrency: two stale copies race to append next');
  const raceId = randomUUID();
  await repository.save(Order.place(raceId, customerId, 15));
  const staleA = await repository.load(raceId);
  const staleB = await repository.load(raceId);
  console.log(`   both loaded at version=${staleA!.version}, status=${staleA!.status}`);

  staleA!.pay();
  await repository.save(staleA!); // advances the stream to version 2 — the only writer that knows about this yet
  console.log('   staleA paid and saved -> stream is now at version 2');

  staleB!.cancel('never saw the payment'); // valid from staleB's own (stale) point of view
  try {
    await repository.save(staleB!);
    console.log('   -> unexpectedly succeeded (lost update!)');
  } catch (error) {
    console.log(`   -> staleB rejected: ${(error as Error).constructor.name}: ${(error as Error).message}`);
    console.log('      (this is exactly the lost-update case optimistic concurrency exists to prevent)');
  }

  console.log('\n6) Projection query — something the event log alone cannot answer directly');
  const summary = await projection.findById(orderId);
  console.log(`   order_projection row for order 1: status=${summary?.status} version=${summary?.version}`);
  const paidOrders = await projection.listByStatus('Paid');
  console.log(`   orders currently Paid: ${paidOrders.length}`);

  await app.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
