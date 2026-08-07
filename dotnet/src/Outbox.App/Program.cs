using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Outbox.App.Cqrs;
using Outbox.App.Orders;
using Outbox.Infrastructure;

var connectionString = Environment.GetEnvironmentVariable("OUTBOX_DOTNET_CONNECTION_STRING")
    ?? "Host=localhost;Port=5433;Database=outbox_dotnet;Username=demo;Password=demo";

var services = new ServiceCollection();
services.AddLogging();
services.AddDbContext<AppDbContext>(options => options.UseNpgsql(connectionString));
services.AddMediatR(cfg =>
{
    cfg.RegisterServicesFromAssembly(typeof(PlaceOrderCommand).Assembly);
    cfg.AddOpenBehavior(typeof(UnitOfWorkBehavior<,>));
});

await using var provider = services.BuildServiceProvider();

// Every call gets its own DI scope -> its own AppDbContext instance, exactly
// like one HTTP request would in a real API. That matters for Scenario 2:
// without a fresh DbContext per command, EF's own change tracker (not
// Postgres) would be the one complaining about the duplicate key.
async Task<TResponse> Send<TResponse>(IRequest<TResponse> request)
{
    await using var scope = provider.CreateAsyncScope();
    var mediator = scope.ServiceProvider.GetRequiredService<ISender>();
    return await mediator.Send(request);
}

async Task SendVoid(IRequest request)
{
    await using var scope = provider.CreateAsyncScope();
    var mediator = scope.ServiceProvider.GetRequiredService<ISender>();
    await mediator.Send(request);
}

Console.WriteLine("=== Scenario 1: success path — Place -> Pay, then a separate order that gets Cancelled ===\n");

var paidOrderId = Guid.NewGuid();
var customerId = Guid.NewGuid();

await Send(new PlaceOrderCommand(paidOrderId, customerId, 149.99m));
await SendVoid(new PayOrderCommand(paidOrderId));

var paid = await Send(new GetOrderByIdQuery(paidOrderId));
Console.WriteLine($"Order {paidOrderId} -> status={paid?.Status}, version={paid?.Version}");
await PrintOutbox(provider, paidOrderId);

var cancelledOrderId = Guid.NewGuid();
await Send(new PlaceOrderCommand(cancelledOrderId, customerId, 40.00m));
await SendVoid(new CancelOrderCommand(cancelledOrderId, "customer changed their mind"));

var cancelled = await Send(new GetOrderByIdQuery(cancelledOrderId));
Console.WriteLine($"\nOrder {cancelledOrderId} -> status={cancelled?.Status}, version={cancelled?.Version}");
await PrintOutbox(provider, cancelledOrderId);

Console.WriteLine("\n=== Scenario 2: rollback proof — force a DB-level failure inside the same SaveChanges ===\n");

var duplicateId = Guid.NewGuid();

// First Place succeeds — one OrderPlaced row lands in the outbox.
await Send(new PlaceOrderCommand(duplicateId, customerId, 10m));
Console.WriteLine($"First Place({duplicateId}) committed.");
await PrintOutbox(provider, duplicateId);

// Second Place reuses the same order_id on purpose, from a brand-new scope
// (brand-new AppDbContext, no in-memory identity-map conflict). The domain
// method happily raises OrderPlaced again — it has no idea the id collides —
// the handler adds the new Order to its DbSet, and only when
// UnitOfWorkBehavior's SaveChangesAsync hits the database does Postgres
// reject the duplicate primary key. Because the order insert and its outbox
// row are one SaveChanges call, BOTH are rolled back together.
try
{
    await Send(new PlaceOrderCommand(duplicateId, customerId, 999m));
    Console.WriteLine("ERROR: expected a primary key violation but none occurred.");
}
catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: "23505" })
{
    Console.WriteLine($"Second Place({duplicateId}) rejected by Postgres (23505 unique_violation), as expected.");
}

Console.WriteLine("Outbox after the rejected second Place (row count must be unchanged, still 1):");
await PrintOutbox(provider, duplicateId);

static async Task PrintOutbox(IServiceProvider provider, Guid aggregateId)
{
    await using var scope = provider.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    var rows = await db.OutboxMessages
        .AsNoTracking()
        .Where(m => m.AggregateId == aggregateId)
        .OrderBy(m => m.Position)
        .ToListAsync();

    foreach (var row in rows)
    {
        Console.WriteLine($"  outbox#{row.Position} {row.EventType} occurred_at={row.OccurredAt:O} payload={row.Payload}");
    }

    if (rows.Count == 0)
    {
        Console.WriteLine("  (no outbox rows for this aggregate)");
    }
}
