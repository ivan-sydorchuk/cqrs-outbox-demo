using Outbox.Domain.Abstractions;

namespace Outbox.Domain.Orders.Events;

public sealed record OrderCancelled(
    Guid AggregateId,
    string Reason,
    DateTimeOffset OccurredAt
) : IDomainEvent;
