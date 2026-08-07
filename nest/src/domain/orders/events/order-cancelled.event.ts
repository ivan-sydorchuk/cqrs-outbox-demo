import { DomainEvent } from '../../domain-event';

export class OrderCancelledEvent implements DomainEvent {
  constructor(
    public readonly aggregateId: string,
    public readonly reason: string,
    public readonly occurredAt: Date,
  ) {}
}
