import { DomainEvent } from '../../domain-event';

export class OrderPlacedEvent implements DomainEvent {
  constructor(
    public readonly aggregateId: string,
    public readonly customerId: string,
    public readonly amount: number,
    public readonly occurredAt: Date,
  ) {}
}
