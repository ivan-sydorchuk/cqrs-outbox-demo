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

A concrete handler (e.g. `PlaceOrderHandler`) only implements `handle()`: call a domain method, write its own row through the transactional Prisma client `tx`, and return which aggregate(s) it touched. `persistOutbox` pulls `getUncommittedEvents()` off each of those aggregates and inserts them into the `outbox` table using the **same `tx`**, then calls `commit()` to clear them — all before the `$transaction` callback resolves and Postgres commits. If anything inside `handle()` throws (including a DB constraint violation), Prisma rolls back everything the transaction did up to that point, and `persistOutbox` is never reached.

(The insert is a raw `$executeRaw` per event rather than `outbox.createMany(...)` — Block 10 added an `Unsupported("xid8")` column to the `Outbox` model, and Prisma disables batch write methods on any model with an Unsupported field. `transaction_id` itself is never set here: a DB-level `DEFAULT pg_current_xact_id()` fills it in — see Block 10 below.)

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

---

# Block 10 — Outbox Worker (relay to Kafka)

Block 9 got domain events *into* the database atomically. This block gets them *out* reliably: a separate
worker process reads unpublished rows from `outbox` and produces them to Kafka, scaled to multiple
instances, crash-safe, and ordered correctly even when transactions commit out of order. Scope, per the
task: this extends the NestJS/Prisma app only — `outbox_dotnet` is untouched.

## The three required questions

**(a) Can this worker scale to multiple instances, and how?**

Yes — the unit of horizontal scaling is the **shard**, not the process. `event_subscription` has one row
per shard (`kafka-order-events-0`, `kafka-order-events-1`, ... — count set by `WORKER_SHARD_COUNT`, 2 in
the demo). An outbox row belongs to shard `abs(hashtext(aggregate_id::text)) % shardCount`. Every worker
instance loops over *all* shard indices every tick and tries `SELECT ... FOR UPDATE SKIP LOCKED` on each
one's subscription row (`src/worker/outbox-relay.ts`); whichever shards aren't currently locked by another
instance, it claims and processes. With N shards, up to N instances can be genuinely active in parallel —
demonstrated live with 2 shards and 2 instances (`A`, `B`), whose logs showed `A` on shard 0 and `B` on
shard 1 running concurrently, not one active + one idle standby. Adding instances beyond the shard count
just gives you spares that pick up work automatically the moment a shard frees up — which is exactly what
happened in the crash test below.

**(b) What happens if an instance fails, and what guarantee do you provide?**

The whole cycle — claim the shard's row lock, read a batch, publish to Kafka, advance the checkpoint — is
one Prisma interactive transaction (`prisma.$transaction(...)` in `pollShard`). If the process dies at any
point before that transaction commits — including *after* Kafka has already acknowledged the batch — the
transaction never commits: Postgres detects the dropped connection, rolls back everything including the
still-open row lock, and any surviving (or later-restarted) instance can immediately claim that shard and
read the exact same batch again. That is **at-least-once**, not exactly-once (the Kafka-ack + checkpoint-
commit is itself a dual-write and can't be made atomic without 2PC) — so the Kafka message carries
`position` as an `event-id` header, and consumers are expected to dedupe on it.

Proven, not just asserted: with two workers running and a burst of 150 orders (250 outbox rows: Place+Pay,
Place+Cancel, or Place-only) in flight, worker `B` was `taskkill /F`'d roughly 4 seconds into the ~10.7s
burst — a hard kill, not a graceful stop. Worker `A` picked up `B`'s abandoned shard within its next poll
tick and finished draining both. An independent Kafka consumer (`scripts/kafka-verify.ts`, a **fresh**
consumer group reading `--from-beginning`, checked against the database — not our own worker's logs)
reported:

```
Consumed 250 messages (250 distinct positions, 0 duplicates).
Position range observed: 1 .. 250
NO GAPS: every position from min to max was observed exactly once (after de-duping retries).
Per-aggregate order preserved across all 150 aggregates.
```

250/250 matches `SELECT count(*) FROM outbox` exactly. Zero duplicates in this particular run means `B`
happened to die before any batch of its got far enough to be re-read — duplicates *are* expected in
general (that's what "at-least-once" means) and the verify script explicitly tolerates and reports them
rather than treating them as a failure; the property it actually enforces is **no gap**.

**(c) How do you process a large backlog efficiently?**

Three things, in order of impact: **(1)** each poll reads a bounded batch (`LIMIT WORKER_BATCH_SIZE`,
default 20) rather than the whole backlog at once. **(2)** the read is `ORDER BY transaction_id, position`
against `idx_outbox_transaction_id (transaction_id, position)` — a single ordered index range scan starting
exactly at the shard's checkpoint, not a table scan. **(3)** sharding means a large backlog is divided
across every currently-active instance rather than drained serially by one worker — add instances (up to
`shardCount`) and backlog drain time drops roughly proportionally. What this implementation does **not**
do: `LISTEN`/`NOTIFY` or CDC (see comparison below) — it polls on a fixed interval
(`WORKER_POLL_INTERVAL_MS`, default 500ms), which bounds lag to the interval and spends idle queries when
there's nothing to do. That's a deliberate, documented scope cut, not an oversight — see below.

## Design notes

**Subscription / checkpoint design.** `event_subscription(name, last_transaction_id xid8, last_position
bigint)` — one row per shard, seeded by `sql/block10-worker-extension.sql`. A restart resumes from
`(last_transaction_id, last_position)` rather than replaying from zero. A **new** subscription (not covered
by this task, but worth flagging per the reading) starts at `('0', 0)` and its first pass is effectively
"the entire table" — fine here, would need explicit backlog-aware batching/throttling at real volume.

**`SKIP LOCKED` concurrency model.** Locking the *subscription row*, not the outbox rows themselves, is
what makes this safe: two instances racing for the same shard get one winner and one instant no-op (`SKIP
LOCKED` never blocks), so there's no risk of a stalled instance holding up another one's progress on a
*different* shard. The tradeoff, made explicitly rather than by accident: the row lock is held for the
**entire** cycle including the Kafka round trip (not released between "claim" and "publish"). That keeps
claim → read → publish → checkpoint trivially consistent (nothing else can touch this shard mid-cycle) at
the cost of a lock held for a network call's duration — acceptable at this volume, and the first thing to
change (toward a lease-with-expiry claim) if throughput ever demanded decoupling them.

**The transaction-ID ordering decision.** `scripts/demo-ordering-bug.ts` reproduces, against a real
Postgres instance, exactly why `ORDER BY position` (the `BIGSERIAL` primary key) alone is unsafe: it opens
a slow transaction that inserts outbox `position=1` and holds without committing, then a fast concurrent
transaction that inserts and commits `position=2` immediately. A naive `SELECT ... ORDER BY position`
poller sees only position 2 at that point (position 1 is still invisible under READ COMMITTED) and would
advance its cursor to 2 — permanently skipping position 1 once it finally commits, since `position > 2`
now excludes it forever. Real output from that script:

```
NAIVE poll — "SELECT ... ORDER BY position" — while A is still uncommitted:
  position=2 aggregate_id=a3e55dc3-...
  A naive worker sees position 2 as the newest row ... and would advance its cursor there.
Transaction A has now committed. Its outbox row exists in the table — but:
  A's row is at position=1, which is <= the cursor (2) already advanced past.
  A "WHERE position > cursor" poller will NEVER select this row again. Event permanently lost.
```

The fix in `pollShard`: order and filter by `(transaction_id, position)`, and additionally require
`transaction_id < pg_snapshot_xmin(pg_current_snapshot())` — strictly below the oldest transaction still in
flight anywhere in the database. That second condition is what actually closes the gap: it refuses to read
*any* row, however high its position, if there's still a chance an older, uncommitted transaction could
insert something the cursor would need to see first.

**Polling vs `LISTEN`/`NOTIFY`.** Shipped: polling, on a fixed interval, per the "ship polling first, know
the upgrade path" note. It's simple, and its failure modes are easy to reason about (worst case: one
interval of extra lag). The upgrade path, not implemented here: a trigger on `outbox` that does `NOTIFY
outbox_channel`, with the worker holding a `LISTEN` connection and treating a notification as "poll now"
rather than waiting out the interval — cuts both lag (near-instant instead of ≤ interval) and idle queries
(no query at all when nothing's happening), at the cost of one more moving part (a trigger + a held
connection) to operate.

## Polling publisher vs. transaction log tailing / CDC

This worker is a **polling publisher**: it queries `outbox` itself, on its own schedule, using its own
transactions. The alternative — covered in Block 9's reading list (Oskar Dudycz, "Push-based Outbox with
Postgres Logical Replication") — is **transaction log tailing**: enable `wal_level = logical`, put `outbox`
in a `PUBLICATION`, create a replication slot, and let Postgres *push* committed rows to a decoder
(`pgoutput`/`wal2json`, or Debezium) as they land in the WAL, with no polling query at all.

Tradeoffs, concretely:

- **Impact on the database.** Polling issues real queries against `outbox` (and briefly locks a
  subscription row) on every tick, competing for connections and cache with the rest of the app. Log
  tailing reads a stream Postgres is already producing for replication — the database does not know or
  care that a consumer is reading it, which is why the CDC reading calls it the *least* intrusive option on
  the impact spectrum.
- **Lag and idle cost.** Polling's lag is bounded by the interval and it spends a query every tick even
  when idle (mitigated, not eliminated, by `LISTEN`/`NOTIFY`). Logical replication pushes near-instantly and
  costs nothing when idle — the slot just sits there.
- **Ordering is already solved either way.** Both approaches face the same out-of-order-commit problem;
  logical replication sidesteps it structurally, because the WAL **is** commit order — there's no
  `transaction_id` bookkeeping to get right, because the stream can't produce row N+1 before row N's
  transaction has committed.
- **Operational cost.** Polling needs nothing beyond the table and this worker. Logical replication needs
  `wal_level = logical` (a server restart to change), a publication, a replication slot that must be
  monitored (an unconsumed slot prevents WAL cleanup and can fill the disk), and a decoder.

**When to switch:** once polling's database load or lag becomes the bottleneck — high write volume, many
shards/subscriptions all polling the same table, or a latency requirement tighter than a sane poll interval
can deliver. For this exercise's volume, polling is the right default; logical replication is the
documented next step, not a requirement here.

## Running the worker demo

```bash
docker compose up -d postgres kafka   # from the repo root

# terminal 1
WORKER_ID=A npm run worker
# terminal 2
WORKER_ID=B npm run worker

# terminal 3 — fires a burst of commands through the normal CQRS command bus
npm run demo:burst

# kill one instance mid-burst from another terminal to see the failover:
#   Windows:      taskkill /F /PID <pid from the "[B] started" log line>
#   macOS/Linux:  kill -9 <pid>

# once things settle, verify independently from Kafka (not from the worker's own logs):
npm run kafka:verify

# to see the ordering bug this design avoids, reproduced against real Postgres:
npm run demo:ordering-bug
```
