namespace Outbox.Domain.Abstractions;

/// <summary>
/// The one marker every aggregate implements. It is what lets a single, generic
/// piece of infrastructure (see Outbox.Infrastructure.AppDbContext.SaveChangesAsync)
/// find "whatever aggregates were touched this unit of work" via
/// ChangeTracker.Entries&lt;IAggregate&gt;() without knowing Order, Account, or
/// anything else exists. No per-aggregate wiring is needed anywhere else.
/// </summary>
public interface IAggregate
{
    IReadOnlyCollection<IDomainEvent> DomainEvents { get; }

    void ClearDomainEvents();
}
