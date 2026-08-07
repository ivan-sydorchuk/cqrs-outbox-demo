# cqrs-outbox-demo

Block 9 practice task: **read/write separation (CQS/CQRS) + a transactional outbox**, built twice against
the same Postgres schema — once in .NET (MediatR + EF Core), once in NestJS (`@nestjs/cqrs` + Prisma) —
to compare how each stack makes domain-event persistence automatic and invisible to the feature developer.

- [`dotnet/`](dotnet/README.md) — MediatR + EF Core
- [`nest/`](nest/README.md) — `@nestjs/cqrs` + Prisma

Each has its own README with the details specific to that stack. This one covers what's shared.

## The one guarantee this whole task is about

> Write your domain events in the **same transaction** as the state change that produced them.

Everything else — read models, projections, CDC, publishing — is downstream of that atomic write. Both
implementations here stop exactly there: they persist domain events to an append-only `outbox` table
atomically with the aggregate's own row. **Nothing publishes anything yet** — that's a later block.

## The aggregate

Both stacks implement the same `Order` aggregate with the same three domain methods and events:

| Method | Event | Guard |
|---|---|---|
| `Place(orderId, customerId, amount)` | `OrderPlaced` | amount must be positive |
| `Pay()` | `OrderPaid` | only from `Pending` |
| `Cancel(reason)` | `OrderCancelled` | not allowed once `Paid` |

## Shared schema, not a shared runtime schema

"Same Postgres schema targeted by both" is satisfied like this: one Postgres instance, two databases —
`outbox_dotnet` and `outbox_nest` — both created from the **exact same DDL**
([`sql/schema.sql`](sql/schema.sql), applied verbatim to each by
[`sql/init-databases.sql`](sql/init-databases.sql)):

```sql
CREATE TABLE orders (
    order_id     UUID PRIMARY KEY,
    customer_id  UUID NOT NULL,
    amount       NUMERIC(12, 2) NOT NULL,
    status       TEXT NOT NULL,
    version      INTEGER NOT NULL DEFAULT 1,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    position     BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Two databases rather than one shared database was a deliberate choice, not a shortcut: EF Core migrations
and Prisma migrations each want to own their schema's history (`__EFMigrationsHistory` /
`_prisma_migrations`), and letting two different migration engines fight over one physical schema would
recreate the exact dual-write problem this task is about, just one layer up. Neither ORM actually owns
migrations here anyway — the tables are created once, up front, by the init script, and both `AppDbContext`
(EF Core) and `schema.prisma` (Prisma) are configured to map onto pre-existing tables rather than generate
DDL. That's what makes "same schema" a provable fact instead of "two schemas that happen to look similar."

## Where the magic lives, side by side

| | .NET | NestJS |
|---|---|---|
| Aggregate base | `AggregateRoot` (hand-rolled, `src/Outbox.Domain/Abstractions`) | `AggregateRoot` (built into `@nestjs/cqrs`) |
| Aggregate marker | `IAggregate` — lets infra find touched aggregates generically | same role, built into `@nestjs/cqrs`'s `AggregateRoot` |
| Command marker | `ICommandBase` / `ICommand` (`Outbox.App/Cqrs`) | `ICommand` (built into `@nestjs/cqrs`) |
| The hidden infra | `AppDbContext.SaveChangesAsync` override + `UnitOfWorkBehavior` pipeline behavior | `OutboxCommandHandler` base class (`$transaction` + pull events + `commit()`) |
| Same-transaction guarantee | EF Core auto-wraps multi-statement `SaveChanges` in one transaction | explicit `prisma.$transaction(async (tx) => ...)` |

In both, a feature developer's command handler **only** loads/creates an aggregate, calls a domain method,
and writes the aggregate's own row. Neither handler ever imports anything with "outbox" in the name.

## Proof of atomicity (both stacks, verified against the database)

Each implementation's demo runs two scenarios:

1. **Success path** — Place → Pay one order, Place → Cancel another. Every domain event lands in the
   outbox in the same transaction as the state change.
2. **Rollback proof** — Place an order, then deliberately try to Place a *second* order reusing the same
   `order_id`. The domain method doesn't validate uniqueness (it can't know), so it happily raises
   `OrderPlaced` again — the only thing that actually fails is the database's primary-key constraint, and
   it fails **inside the same transaction** that would have written the outbox row. Both implementations
   catch that failure (`23505`/EF `DbUpdateException` on .NET, `P2002`/`PrismaClientKnownRequestError` on
   Nest) and then re-query the outbox to show the row count is unchanged.

Verified counts after a full run, queried directly with `psql` (not through either app):

```
outbox_dotnet: orders = 3 rows, outbox = 5 rows
outbox_nest:   orders = 3 rows, outbox = 5 rows
```

3 orders because the duplicate `Place` never landed; 5 outbox rows because 2 + 2 + 1 events were raised
and the rejected duplicate contributed zero — not "5 minus something we cleaned up after the fact," it
never existed in the first place.

## Running everything

```bash
docker compose up -d postgres   # one Postgres container, two databases: outbox_dotnet, outbox_nest

cd dotnet && dotnet run --project src/Outbox.App/Outbox.App.csproj

cd nest && npm install && npx prisma generate && npm run start
```

See each subproject's README for what its output looks like and the exact code a feature developer would
add for a new event-producing command.
