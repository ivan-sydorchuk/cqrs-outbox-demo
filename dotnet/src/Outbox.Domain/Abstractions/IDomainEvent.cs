namespace Outbox.Domain.Abstractions;

/// <summary>
/// Marker for anything an aggregate raises as a fact about something that happened.
/// This is what ends up serialized into the outbox's payload column.
/// </summary>
public interface IDomainEvent
{
    Guid AggregateId { get; }
    DateTimeOffset OccurredAt { get; }
}
