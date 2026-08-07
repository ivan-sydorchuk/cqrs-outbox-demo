import 'dotenv/config';
import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { CommandBus, QueryBus } from '@nestjs/cqrs';
import { randomUUID } from 'crypto';
import { Prisma } from '@prisma/client';
import { OrdersModule } from './orders.module';
import { PrismaService } from './infrastructure/prisma.service';
import { PlaceOrderCommand } from './application/orders/place-order.command';
import { PayOrderCommand } from './application/orders/pay-order.command';
import { CancelOrderCommand } from './application/orders/cancel-order.command';
import { GetOrderByIdQuery, OrderDto } from './application/orders/get-order-by-id.query';

@Module({ imports: [OrdersModule] })
class AppModule {}

async function printOutbox(prisma: PrismaService, aggregateId: string): Promise<void> {
  const rows = await prisma.outbox.findMany({
    where: { aggregateId },
    orderBy: { position: 'asc' },
  });

  if (rows.length === 0) {
    console.log('  (no outbox rows for this aggregate)');
    return;
  }

  for (const row of rows) {
    console.log(
      `  outbox#${row.position} ${row.eventType} occurred_at=${row.occurredAt.toISOString()} payload=${JSON.stringify(row.payload)}`,
    );
  }
}

async function main(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
  const commandBus = app.get(CommandBus);
  const queryBus = app.get(QueryBus);
  const prisma = app.get(PrismaService);

  console.log('=== Scenario 1: success path — Place -> Pay, then a separate order that gets Cancelled ===\n');

  const paidOrderId = randomUUID();
  const customerId = randomUUID();

  await commandBus.execute(new PlaceOrderCommand(paidOrderId, customerId, 149.99));
  await commandBus.execute(new PayOrderCommand(paidOrderId));

  const paid = await queryBus.execute<GetOrderByIdQuery, OrderDto | null>(new GetOrderByIdQuery(paidOrderId));
  console.log(`Order ${paidOrderId} -> status=${paid?.status}, version=${paid?.version}`);
  await printOutbox(prisma, paidOrderId);

  const cancelledOrderId = randomUUID();
  await commandBus.execute(new PlaceOrderCommand(cancelledOrderId, customerId, 40.0));
  await commandBus.execute(new CancelOrderCommand(cancelledOrderId, 'customer changed their mind'));

  const cancelled = await queryBus.execute<GetOrderByIdQuery, OrderDto | null>(
    new GetOrderByIdQuery(cancelledOrderId),
  );
  console.log(`\nOrder ${cancelledOrderId} -> status=${cancelled?.status}, version=${cancelled?.version}`);
  await printOutbox(prisma, cancelledOrderId);

  console.log('\n=== Scenario 2: rollback proof — force a DB-level failure inside the same transaction ===\n');

  const duplicateId = randomUUID();

  // First Place succeeds — one OrderPlaced row lands in the outbox.
  await commandBus.execute(new PlaceOrderCommand(duplicateId, customerId, 10));
  console.log(`First Place(${duplicateId}) committed.`);
  await printOutbox(prisma, duplicateId);

  // Second Place reuses the same order_id on purpose: the domain method
  // happily raises OrderPlaced again (it has no idea the id collides), and
  // OutboxCommandHandler.execute runs the whole thing inside one
  // prisma.$transaction. tx.order.create() is what actually hits Postgres and
  // gets rejected with a unique-constraint violation (P2002) — since that
  // throw happens inside the same interactive transaction that would have
  // held the outbox insert, Prisma rolls the whole transaction back and the
  // outbox insert never happens at all.
  try {
    await commandBus.execute(new PlaceOrderCommand(duplicateId, customerId, 999));
    console.log('ERROR: expected a unique constraint violation but none occurred.');
  } catch (error) {
    if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === 'P2002') {
      console.log(`Second Place(${duplicateId}) rejected by Postgres (P2002 unique constraint), as expected.`);
    } else {
      throw error;
    }
  }

  console.log('Outbox after the rejected second Place (row count must be unchanged, still 1):');
  await printOutbox(prisma, duplicateId);

  await app.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
