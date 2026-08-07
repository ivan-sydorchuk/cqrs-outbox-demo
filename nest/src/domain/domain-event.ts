/**
 * Equivalent of the .NET side's IDomainEvent: the minimum shape the outbox
 * needs to turn an event into a row (aggregate_id, occurred_at — event_type
 * comes from the class name, payload from JSON.stringify(event)).
 */
export interface DomainEvent {
  readonly aggregateId: string;
  readonly occurredAt: Date;
}
