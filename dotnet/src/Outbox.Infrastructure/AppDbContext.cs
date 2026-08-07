using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Outbox.Domain.Abstractions;
using Outbox.Domain.Orders;

namespace Outbox.Infrastructure;

/// <summary>
/// This is where the "magic" from the practice task lives.
///
/// <see cref="SaveChangesAsync"/> is the ONLY place that knows the outbox table
/// exists. It asks the change tracker for every tracked <see cref="IAggregate"/>
/// that has uncommitted domain events, turns each event into an
/// <see cref="OutboxMessage"/> row, and adds those rows to the SAME
/// unit of work as whatever aggregate mutation triggered them. A single call to
/// the base SaveChangesAsync then commits both the write-model change and the
/// outbox rows atomically — EF Core wraps multi-statement SaveChanges calls in
/// an implicit database transaction on its own, so no manual
/// BeginTransaction/Commit is needed here for that guarantee.
///
/// No command handler, and no aggregate, ever references OutboxMessage or this
/// class's internals. They only see IAggregate/AggregateRoot.
/// </summary>
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(builder =>
        {
            builder.ToTable("orders");
            builder.HasKey(o => o.OrderId);
            builder.Property(o => o.OrderId).HasColumnName("order_id");
            builder.Property(o => o.CustomerId).HasColumnName("customer_id");
            builder.Property(o => o.Amount).HasColumnName("amount").HasColumnType("numeric(12,2)");
            builder.Property(o => o.Status).HasColumnName("status").HasConversion<string>();
            builder.Property(o => o.Version).HasColumnName("version").IsConcurrencyToken();
            builder.Property(o => o.CreatedAt).HasColumnName("created_at");
            builder.Property(o => o.UpdatedAt).HasColumnName("updated_at");
        });

        modelBuilder.Entity<OutboxMessage>(builder =>
        {
            builder.ToTable("outbox");
            builder.HasKey(o => o.Position);
            builder.Property(o => o.Position).HasColumnName("position").ValueGeneratedOnAdd();
            builder.Property(o => o.AggregateId).HasColumnName("aggregate_id");
            builder.Property(o => o.EventType).HasColumnName("event_type");
            builder.Property(o => o.Payload).HasColumnName("payload").HasColumnType("jsonb");
            builder.Property(o => o.OccurredAt).HasColumnName("occurred_at");
        });
    }

    public override async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        var touchedAggregates = ChangeTracker.Entries<IAggregate>()
            .Select(entry => entry.Entity)
            .Where(aggregate => aggregate.DomainEvents.Count > 0)
            .ToList();

        var outboxRows = touchedAggregates
            .SelectMany(aggregate => aggregate.DomainEvents)
            .Select(domainEvent => new OutboxMessage
            {
                AggregateId = domainEvent.AggregateId,
                EventType = domainEvent.GetType().Name,
                Payload = JsonSerializer.Serialize(domainEvent, domainEvent.GetType()),
                OccurredAt = domainEvent.OccurredAt,
            })
            .ToList();

        if (outboxRows.Count > 0)
        {
            OutboxMessages.AddRange(outboxRows);
        }

        // Single SaveChanges call = one transaction covering the aggregate's
        // state change AND every outbox row derived from it.
        var affected = await base.SaveChangesAsync(cancellationToken);

        // Only reached on success, so a thrown/rolled-back save leaves the
        // events sitting on the aggregate for a caller to retry rather than
        // silently dropping them.
        foreach (var aggregate in touchedAggregates)
        {
            aggregate.ClearDomainEvents();
        }

        return affected;
    }
}
