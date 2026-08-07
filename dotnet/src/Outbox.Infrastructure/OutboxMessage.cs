namespace Outbox.Infrastructure;

/// <summary>EF Core mapping for the append-only "outbox" table. Never updated, never deleted.</summary>
public sealed class OutboxMessage
{
    public long Position { get; set; }
    public Guid AggregateId { get; set; }
    public string EventType { get; set; } = null!;
    public string Payload { get; set; } = null!; // JSON text, column type jsonb
    public DateTimeOffset OccurredAt { get; set; }
}
