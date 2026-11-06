# event-sourcing

Block 11 practice task: an event-sourced `Order` aggregate — state is never stored, only *computed* by
folding its event stream — built once against a storage-agnostic `EventStore` interface, then run
unmodified against two implementations: Postgres and KurrentDB.

This is a separate subproject from [`dotnet/`](../dotnet) and [`nest/`](../nest) on purpose. Those two
are CQRS + transactional outbox (Blocks 9–10) on top of plain CRUD — `orders` is a real row holding
current state, and `outbox` just reliably publishes the events that row's changes produced. Here there
is no `orders` row at all. See the mentor note: **Event Sourcing ≠ CQRS ≠ Outbox** — conflating "an
event-sourced aggregate" with "an aggregate that happens to raise events" is exactly the mistake a
shared subproject would invite.

## The aggregate

[`src/domain/order.aggregate.ts`](src/domain/order.aggregate.ts) — same three operations as Blocks 9–10,
but the shape of the class is different in one structural way:

| Method | Event | Guard |
|---|---|---|
| `Order.place(orderId, customerId, amount)` | `OrderPlacedEvent` | amount must be positive |
| `pay()` | `OrderPaidEvent` | only from `Pending` |
| `cancel(reason)` | `OrderCancelledEvent` | not allowed once `Paid` |

`place`/`pay`/`cancel` validate and construct an event — nothing else. The **only** code that ever
touches `orderId`/`customerId`/`amount`/`status`/`version` is the private `apply(event)` method, called
both for a brand-new event and for every event replayed out of the store. Compare this to
[`nest/src/domain/orders/order.aggregate.ts`](../nest/src/domain/orders/order.aggregate.ts) (Block 9),
where `pay()` sets `this.status = 'Paid'` directly and `apply()` is just notification after the fact —
that one line is the entire difference between "an aggregate that raises events" and "an aggregate whose
state *is* its events."

`Order.replay(events, fromSnapshot?)` is `load()`'s other half: fold a stream (optionally starting from a
snapshot) into a fresh instance. There is no `Order.hydrate(row)` here, because there is no row.

## Optimistic concurrency

[`src/order.repository.ts`](src/order.repository.ts) computes `expectedVersion` as "the stream's version
before the events this aggregate just raised" and hands it to `EventStore.append`. Both store
implementations reject the whole batch with `ConcurrencyError` if the stream has moved since the
aggregate was loaded:

- **Postgres** ([`postgres-event-store.ts`](src/event-store/postgres-event-store.ts)): `SELECT
  MAX(version) ... FOR UPDATE` inside the append transaction as the primary check, with
  `order_events`'s `PRIMARY KEY (stream_id, version)` as the backstop that catches the race no row-lock
  could — two concurrent first-appends to a brand-new stream, where there's nothing yet to lock.
- **KurrentDB** ([`kurrentdb-event-store.ts`](src/event-store/kurrentdb-event-store.ts)): native
  append-with-expected-revision; a `WrongExpectedVersionError` is translated to the same
  `ConcurrencyError`.

`src/demo.ts` step 5 provokes this for real: two stale in-memory copies of the same order, one pays
(succeeds, advances the stream), the other then tries to cancel using its now-stale version — rejected.
That's the lost-update case Block 8 already covered, showing up again because event sourcing's append is
just that same check applied to a log instead of a row.

## Storage abstraction

[`src/event-store/event-store.ts`](src/event-store/event-store.ts) is the entire contract:

```ts
interface EventStore {
  append(streamId: string, expectedVersion: number, events: NewEvent[]): Promise<void>;
  readStream(streamId: string, options?: { fromVersion?: number; toVersion?: number }): Promise<StoredEvent[]>;
}
```

`toVersion` is the temporal query — "state as of version N" is just a narrower replay, not a separate
feature (`src/demo.ts` step 4). `fromVersion` is what `OrderRepository.load` passes as the snapshot's own
version, so the *store* only fetches events the snapshot hasn't folded yet — the first version of this
interface only took `toVersion`, which meant a snapshot cut fold time but not read time: `load()` was
still fetching the entire stream from the store every call and filtering client-side. `scripts/verify-gaps.ts`
is what caught this. [`app.module.ts`](src/app.module.ts) binds the `EVENT_STORE` DI token to one or the
other based on an env var:

```ts
provide: EVENT_STORE,
useClass: process.env.EVENT_STORE === 'kurrentdb' ? KurrentDbEventStore : PostgresEventStore,
```

`OrderRepository` and `Order` import neither class. Swapping stores is that one line plus the env var —
no change to `order.repository.ts`, `order.aggregate.ts`, or `demo.ts`.

## Bonus: snapshot + projection

- [`PostgresSnapshotStore`](src/snapshot/postgres-snapshot-store.ts) — every `SNAPSHOT_EVERY` events,
  `OrderRepository.save` writes `{orderId, customerId, amount, status, version}` to `order_snapshots`
  (an upsert — the one legitimate `UPDATE` in this subproject, because a snapshot is a cache, not part of
  the log). `load()` starts from the snapshot and only replays events after it. Delete every row in
  `order_snapshots` and every `load()` call still returns the identical aggregate, just slower.
- [`OrderSummaryProjection`](src/projection/order-summary.projection.ts) — `order_projection`, a read
  model kept in sync by folding, with a query the event log genuinely cannot answer on its own:
  `listByStatus('Paid')` (`demo.ts` step 6). The demo updates it inline, right after
  `EventStore.append` succeeds, in the same call — see the one simplification called out below.

## What problems arise with this approach?

1. **Replay cost grows with the stream.** `load()` is O(events in the stream). Fine at tens of events,
   a real problem at hundreds of thousands appended to one long-lived aggregate. The fix here is the
   snapshot above — but per the "don't CRUD-source" sibling warning (*don't snapshot prematurely*), it's
   only worth adding once replay is actually measured to be slow, not by default.

2. **You cannot query across aggregates from the log.** `order_events` is keyed by `stream_id`; there is
   no efficient "all Paid orders" query against it — you'd have to replay every stream in the database
   and filter in application code. That's what `OrderSummaryProjection` is for, and it comes with a cost:
   the projection is only as fresh as the last time something updated it. This demo updates it inline in
   `OrderRepository.save`, in the same request that appended the event — which is a shortcut that quietly
   reintroduces the dual-write problem Block 10 exists to solve (append to the log and update the
   projection are two separate writes with no transaction tying them together; if the process dies
   between them, the projection is permanently stale for that one event). A real system updates
   projections asynchronously, off a subscription to the store (KurrentDB's native subscriptions) or off
   an outbox-published integration event (Block 10) — never inline in the write path.

3. **Old events outlive their shape.** `OrderPlacedEvent`'s fields are a public commitment the moment the
   first instance is appended — add a required field two months later and every already-stored event is
   missing it. `deserializeOrderEvent` (`src/domain/order-events.ts`) is the one place this would have to
   be handled: either every field it reads has to tolerate being `undefined` on old payloads, or each
   event needs an explicit schema version and an upcast step before the aggregate ever sees it. This demo
   doesn't need it yet because the three events have never changed shape — but it's the first thing that
   breaks in a long-lived real system.

4. **Read models are eventually consistent, full stop.** Even wired up correctly (async, off a
   subscription), there is always a window after `append` succeeds and before the projection reflects it.
   A command handler that writes an event and then immediately reads its own projection to decide what to
   do next is a bug waiting to happen — it has to go back to the aggregate (replay) for anything that
   needs to be strongly consistent with what it just wrote.

5. **Replay must never repeat a side effect.** Nothing in this demo calls an external system, so it
   doesn't come up here — but it's the sharpest warning in the Fowler article for a reason: the moment
   `apply()` or anything in the replay path sends an email, calls a payment gateway, or publishes to
   Kafka, rebuilding an aggregate (or replaying history to fix a bad past event) re-triggers it. Every
   external side effect has to live in a gateway that explicitly knows "I am in replay mode, do nothing,"
   which this subproject deliberately never needed to build because `apply` only ever touches the
   aggregate's own five fields.

## Known limitations (not fixed, called out instead)

Found while writing `scripts/verify-gaps.ts` and fixed: the snapshot's `readStream` call used to fetch
the *whole* stream and filter in memory (see the storage-abstraction section above) — the store-level
`fromVersion` bound is the fix. `KurrentDbEventStore`'s `ConcurrencyError` used to drop the actual
version (`WrongExpectedVersionError.actualState` wasn't being read) while Postgres's included it — now
both report it. What's left, deliberately not fixed because it's out of this task's scope or a reasonable
demo simplification:

- **Snapshot/projection are Postgres-only regardless of `EVENT_STORE`.** Switching to `kurrentdb` swaps
  the *event store*; `PostgresSnapshotStore` and `OrderSummaryProjection` are hardcoded to Postgres. A
  "KurrentDB with nothing else" deployment isn't what this demonstrates — only the append/replay path is
  actually storage-agnostic, which is what requirement 5 asked for.
- **Three separate `pg.Pool`s** (`PostgresEventStore`, `PostgresSnapshotStore`, `OrderSummaryProjection`)
  against the same database instead of one shared pool. Wasteful, not wrong, for a single-process demo.
- **No idempotent event IDs on retry.** Each `append()` call generates fresh UUIDs for KurrentDB's event
  IDs; a network retry after an append that actually succeeded but whose ack was lost would append the
  same events twice instead of KurrentDB's own id-based dedup catching it. Not exercised anywhere here.
- **`OrderSummaryProjection.apply` is a full-state upsert, not an event-by-event projector.** It's called
  once per `save()` with the post-replay aggregate, not with `on(OrderPlaced)`/`on(OrderPaid)` handlers
  reacting to one event type at a time. It's still a legitimate projection (a read model, built from
  events, queryable in ways the log isn't), but it under-sells the general pattern — a projection with a
  genuinely different shape from its aggregate (counts, sums, denormalized joins across streams) would
  need the event-by-event form.
- **No automated tests.** Everything above was proven by running `npm run demo` / `npm run verify-gaps`
  against live containers, not by a test suite — fine for a practice task, not for anything meant to stay
  correct under future changes.

## Running it

```bash
docker compose up -d postgres kurrentdb   # from the repo root

cd event-sourcing
cp .env.example .env
npm install
npm run migrate      # idempotent — safe even if sql/init-databases.sql already ran this once

npm run demo             # EVENT_STORE=postgres (the .env default)
npm run demo:kurrentdb   # identical code, EVENT_STORE=kurrentdb — see app.module.ts

npm run verify-gaps             # genuine concurrent race + snapshot write/read, against Postgres
npm run verify-gaps:kurrentdb   # same two checks, against KurrentDB
```

Both runs execute the exact same `src/demo.ts`, `order.repository.ts`, and `order.aggregate.ts` — only
the env var changes.
