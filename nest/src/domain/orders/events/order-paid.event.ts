import { DomainEvent } from '../../domain-event';

export class OrderPaidEvent implements DomainEvent {
  constructor(
    public readonly aggregateId: string,
    public readonly occurredAt: Date,
  ) {}
}
