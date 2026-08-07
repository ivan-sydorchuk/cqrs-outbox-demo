# .NET implementation — MediatR + EF Core

## The aggregate

`Order` (`src/Outbox.Domain/Orders/Order.cs`) — three domain methods, three events:

| Method | Event | Guard |
|---|---|---|
| `Order.Place(id, customerId, amount)` | `OrderPlaced` | amount must be positive |
| `order.Pay()` | `OrderPaid` | only from `Pending` |
| `order.Cancel(reason)` | `OrderCancelled` | not allowed once `Paid` |

## The marker types

- `IDomainEvent` (`src/Outbox.Domain/Abstractions/IDomainEvent.cs`) — every event carries `AggregateId` + `OccurredAt`, which is all the outbox needs.
- `IAggregate` (`src/Outbox.Domain/Abstractions/IAggregate.cs`) — the one interface that makes the infrastructure generic: `DomainEvents` + `ClearDomainEvents()`. `AggregateRoot` implements it and gives domain methods a protected `Raise(...)`.
- `ICommandBase` / `ICommand` / `ICommand<TResponse>` (`src/Outbox.App/Cqrs/ICommand.cs`) — thin wrappers over MediatR's `IRequest`/`IRequest<T>`. `ICommandBase` is non-generic on purpose: it's what lets the pipeline behavior below constrain itself to "commands only" without needing a matching generic parameter.
- `IQuery<TResponse>` (`src/Outbox.App/Cqrs/IQuery.cs`) — plain `IRequest<T>`, never touches `ICommandBase`, so it never runs through the outbox-saving step.

## Where the magic lives

**Two places, and nowhere else:**

1. **`AppDbContext.SaveChangesAsync`** (`src/Outbox.Infrastructure/AppDbContext.cs`). Before delegating to `base.SaveChangesAsync`, it asks `ChangeTracker.Entries<IAggregate>()` for every tracked aggregate with pending domain events, turns each event into an `OutboxMessage` row via `OutboxMessages.AddRange(...)`, and only then calls the base save. EF Core wraps a multi-statement `SaveChanges` in one implicit database transaction on its own — so the order/aggregate row and every outbox row it produced commit or roll back together, with no manual `BeginTransaction` anywhere in this codebase.
2. **`UnitOfWorkBehavior<TRequest, TResponse>`** (`src/Outbox.App/Cqrs/UnitOfWorkBehavior.cs`), a MediatR pipeline behavior constrained to `TRequest : ICommandBase`. It runs the command handler, then calls `AppDbContext.SaveChangesAsync()` exactly once. Handlers never call `SaveChanges` themselves.

Put together: a command handler loads/creates an aggregate and calls a domain method — full stop. `UnitOfWorkBehavior` decides when to save; `AppDbContext.SaveChangesAsync` decides what "save" actually persists (aggregate state + outbox rows, same transaction). Query handlers (`GetOrderByIdQueryHandler`) don't implement `ICommandBase`, so this pipeline step never runs for them, and they default to `AsNoTracking()`.

## The developer contract in practice

Adding `OrderShipped` requires exactly:

```csharp
// (a) the event
public sealed record OrderShipped(Guid AggregateId, DateTimeOffset OccurredAt) : IDomainEvent;

// (b) the domain method
public void Ship()
{
    if (Status != OrderStatus.Paid) throw new InvalidOperationException(...);
    Status = OrderStatus.Shipped;
    Raise(new OrderShipped(OrderId, DateTimeOffset.UtcNow));
}

// (c) the handler
public sealed record ShipOrderCommand(Guid OrderId) : ICommand;

public sealed class ShipOrderCommandHandler(AppDbContext db) : ICommandHandler<ShipOrderCommand>
{
    public async Task Handle(ShipOrderCommand request, CancellationToken ct)
    {
        var order = await db.Orders.FindAsync([request.OrderId], ct)
            ?? throw new InvalidOperationException("not found");
        order.Ship();
    }
}
```

No outbox reference, no `SaveChanges` call, no transaction handling — that is the whole point.

## Running it

```bash
docker compose up -d postgres   # from the repo root
dotnet run --project src/Outbox.App/Outbox.App.csproj
```

Connects to `outbox_dotnet` on `localhost:5433` by default (override with `OUTBOX_DOTNET_CONNECTION_STRING`). The demo:

1. **Scenario 1** — places, pays, and separately places-then-cancels two orders; prints the resulting outbox rows for each.
2. **Scenario 2 — atomicity proof.** Places an order (1 outbox row). Then, from a fresh DI scope, tries to `Place` a *second* order reusing the same `order_id` on purpose. The domain method has no idea the id collides, so it happily raises `OrderPlaced` again — the failure only happens when `UnitOfWorkBehavior`'s `SaveChangesAsync` hits Postgres and gets a `23505 unique_violation`. Because the order insert and its outbox row were one `SaveChanges` call, **both roll back together**: the outbox row count for that aggregate is verified unchanged (still 1) immediately after the rejected command.

Verified independently against the database after a run:

```
orders   -> 3 rows (Paid, Cancelled, Pending — the duplicate insert never landed)
outbox   -> 5 rows total (2 + 2 + 1; the rejected second Place added zero)
```
