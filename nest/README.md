# NestJS implementation — @nestjs/cqrs + Prisma

## The aggregate

`Order` (`src/domain/orders/order.aggregate.ts`) — three domain methods, three events, mirroring the .NET side:

| Method | Event | Guard |
|---|---|---|
| `Order.place(orderId, customerId, amount)` | `OrderPlacedEvent` | amount must be positive |
| `order.pay()` | `OrderPaidEvent` | only from `Pending` |
| `order.cancel(reason)` | `OrderCancelledEvent` | not allowed once `Paid` |

## The marker types

- `DomainEvent` (`src/domain/domain-event.ts`) — the minimum shape the outbox needs: `aggregateId` + `occurredAt`. Direct equivalent of the .NET side's `IDomainEvent`.
- **Aggregate base class: `@nestjs/cqrs`'s own `AggregateRoot`.** Per the suggested stack, we don't hand-roll one — `AggregateRoot` already gives every aggregate `apply(event)`, `getUncommittedEvents()`, and `commit()`. `Order` just extends it. We deliberately never merge it with an `EventPublisher` context, so `apply()` only queues the event — it never touches NestJS's own `EventBus` (that's publishing, which is explicitly out of scope for this task).
- **Command marker: `@nestjs/cqrs`'s own `ICommand`**, dispatched through `CommandBus`/`@CommandHandler`. Same reasoning — the library already provides exactly the marker the task asks for.
- `IQuery` (also from `@nestjs/cqrs`) for the read side, dispatched through `QueryBus`/`@QueryHandler`.

## Where the magic lives

**One place: `OutboxCommandHandler`** (`src/infrastructure/outbox-command.handler.ts`) — the direct counterpart of `AppDbContext.SaveChangesAsync` on the .NET side. Every command handler extends it instead of implementing NestJS's `execute()` directly:

```ts
export abstract class OutboxCommandHandler<TCommand extends ICommand, TResult = void> {
  async execute(command: TCommand): Promise<TResult> {
    return this.prisma.$transaction(async (tx) => {
      const { result, aggregates } = await this.handle(command, tx);
      await this.persistOutbox(tx, aggregates);   // <- the only place that knows "outbox" exists
      return result;
    });
  }

  protected abstract handle(command: TCommand, tx: Prisma.TransactionClient): Promise<HandleResult<TResult>>;
}
```

A concrete handler (e.g. `PlaceOrderHandler`) only implements `handle()`: call a domain method, write its own row through the transactional Prisma client `tx`, and return which aggregate(s) it touched. `persistOutbox` pulls `getUncommittedEvents()` off each of those aggregates, `createMany`s them into the `outbox` table using the **same `tx`**, then calls `commit()` to clear them — all before the `$transaction` callback resolves and Postgres commits. If anything inside `handle()` throws (including a DB constraint violation), Prisma rolls back everything the transaction did up to that point, and `persistOutbox` is never reached.

Query handlers (`GetOrderByIdHandler`) go straight to the plain `PrismaService` client and never extend `OutboxCommandHandler` — there's nothing for them to save, so they never even see a `Prisma.TransactionClient`.

## The developer contract in practice

Adding `OrderShipped` requires exactly:

```ts
// (a) the event
export class OrderShippedEvent implements DomainEvent {
  constructor(public readonly aggregateId: string, public readonly occurredAt: Date) {}
}

// (b) the domain method
ship(): void {
  if (this.status !== 'Paid') throw new Error(...);
  this.status = 'Shipped';
  this.apply(new OrderShippedEvent(this.orderId, new Date()));
}

// (c) the command + handler
export class ShipOrderCommand implements ICommand {
  constructor(public readonly orderId: string) {}
}

@CommandHandler(ShipOrderCommand)
export class ShipOrderHandler extends OutboxCommandHandler<ShipOrderCommand> {
  constructor(prisma: PrismaService) { super(prisma); }

  protected async handle(command: ShipOrderCommand, tx: Prisma.TransactionClient) {
    const record = await tx.order.findUnique({ where: { orderId: command.orderId } });
    const order = Order.hydrate(toOrderRow(record!));
    order.ship();
    await tx.order.update({ where: { orderId: order.orderId }, data: { status: order.status } });
    return { result: undefined, aggregates: [order] };
  }
}
```

No outbox reference, no `$transaction` wiring, no `commit()` call — that is the whole point.

## Running it

```bash
docker compose up -d postgres   # from the repo root
npm install
npx prisma generate             # generates the client from prisma/schema.prisma (no migration — tables already exist)
npm run start
```

Connects to `outbox_nest` on `localhost:5433` via `OUTBOX_NEST_DATABASE_URL` (see `.env`). The demo:

1. **Scenario 1** — places, pays, and separately places-then-cancels two orders; prints the resulting outbox rows for each.
2. **Scenario 2 — atomicity proof.** Places an order (1 outbox row). Then tries to `Place` a *second* order reusing the same `order_id` on purpose. `tx.order.create()` is the statement that actually hits Postgres and gets rejected with a unique-constraint violation (`P2002`) — since that happens inside the same `prisma.$transaction` that would otherwise have gone on to insert the outbox row, Prisma rolls back the whole transaction and the outbox insert never runs at all.

Verified independently against the database after a run:

```
orders   -> 3 rows (Paid, Cancelled, Pending — the duplicate insert never landed)
outbox   -> 5 rows total (2 + 2 + 1; the rejected second Place added zero)
```

## Note on `prisma/schema.prisma`

It's hand-written to match `sql/schema.sql` exactly rather than produced by `prisma migrate`. The tables are created once, up front, by `sql/init-databases.sql` — Prisma only introspects/maps onto them here (`prisma generate`, no `migrate`), so there's no separate migration history for Prisma to own and no way for its DDL to drift from the canonical shared schema the .NET side also targets.
