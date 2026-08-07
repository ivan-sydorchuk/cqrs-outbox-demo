using Outbox.Domain.Abstractions;

namespace Outbox.Domain.Orders.Events;

public sealed record OrderPaid(
    Guid AggregateId,
    DateTimeOffset OccurredAt
) : IDomainEvent;
