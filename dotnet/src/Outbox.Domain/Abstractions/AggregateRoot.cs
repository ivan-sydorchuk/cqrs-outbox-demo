namespace Outbox.Domain.Abstractions;

/// <summary>
/// Base class for write-model aggregates. Domain methods call <see cref="Raise"/>
/// to append an event; nothing else in the domain layer ever touches the outbox —
/// that happens later, invisibly, in infrastructure.
/// </summary>
public abstract class AggregateRoot : IAggregate
{
    private readonly List<IDomainEvent> _domainEvents = [];

    public IReadOnlyCollection<IDomainEvent> DomainEvents => _domainEvents.AsReadOnly();

    protected void Raise(IDomainEvent domainEvent) => _domainEvents.Add(domainEvent);

    public void ClearDomainEvents() => _domainEvents.Clear();
}
