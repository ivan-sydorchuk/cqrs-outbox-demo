using Outbox.Domain.Abstractions;

namespace Outbox.Domain.Orders.Events;

public sealed record OrderPlaced(
    Guid AggregateId,
    Guid CustomerId,
    decimal Amount,
    DateTimeOffset OccurredAt
) : IDomainEvent;
