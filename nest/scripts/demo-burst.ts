import 'dotenv/config';
import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { CommandBus } from '@nestjs/cqrs';
import { randomUUID } from 'crypto';
import { OrdersModule } from '../src/orders.module';
import { PlaceOrderCommand } from '../src/application/orders/place-order.command';
import { PayOrderCommand } from '../src/application/orders/pay-order.command';
import { CancelOrderCommand } from '../src/application/orders/cancel-order.command';

@Module({ imports: [OrdersModule] })
class BurstAppModule {}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Fires a burst of real CQRS commands through the normal command bus (same
 * path as the Block 9 demo) — this is what stands in for concurrent API
 * traffic while two worker instances are polling in the background. Each
 * order gets 1 or 2 outbox rows (Place, then optionally Pay or Cancel), so
 * the worker has a genuine mix of aggregates/positions/shards to distribute,
 * not just one repeated event type.
 */
async function main() {
  const orderCount = Number(process.env.BURST_ORDER_COUNT ?? 150);
  const delayMs = Number(process.env.BURST_DELAY_MS ?? 40);

  const app = await NestFactory.createApplicationContext(BurstAppModule, { logger: false });
  const commandBus = app.get(CommandBus);
  const customerId = randomUUID();

  console.log(`Starting burst: ${orderCount} orders, ~${delayMs}ms between orders`);
  const startedAt = Date.now();

  for (let i = 0; i < orderCount; i++) {
    const orderId = randomUUID();
    await commandBus.execute(new PlaceOrderCommand(orderId, customerId, 10 + i));

    const roll = i % 3;
    if (roll === 0) {
      await commandBus.execute(new PayOrderCommand(orderId));
    } else if (roll === 1) {
      await commandBus.execute(new CancelOrderCommand(orderId, 'burst demo'));
    }
    // roll === 2: left as Pending — a single-event aggregate on purpose.

    if ((i + 1) % 25 === 0) {
      console.log(`  ${i + 1}/${orderCount} orders placed (${Date.now() - startedAt}ms elapsed)`);
    }

    await sleep(delayMs);
  }

  console.log(`Burst finished in ${Date.now() - startedAt}ms.`);
  await app.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
