/**
 * Business-intent events only (no `OrderUpdated`) -- see the "don't
 * CRUD-source" note. Each event carries exactly the data its own
 * `Order.apply` branch needs, nothing more.
 */

export class OrderPlacedEvent {
  constructor(
    public readonly orderId: string,
    public readonly customerId: string,
    public readonly amount: number,
    public readonly occurredAt: Date,
  ) {}
}

export class OrderPaidEvent {
  constructor(
    public readonly orderId: string,
    public readonly occurredAt: Date,
  ) {}
}

export class OrderCancelledEvent {
  constructor(
    public readonly orderId: string,
    public readonly reason: string,
    public readonly occurredAt: Date,
  ) {}
}

export type OrderEvent = OrderPlacedEvent | OrderPaidEvent | OrderCancelledEvent;

/**
 * event_type in the store is the class name (matches the Block 9/10 outbox
 * convention: `event.constructor.name`, see outbox-command.handler.ts), and
 * payload is `JSON.stringify(event)` -- so serialization needs no explicit
 * mapping here. Only the inverse -- stored (eventType, payload) back into a
 * typed event the aggregate can fold -- needs a table.
 */
export function deserializeOrderEvent(eventType: string, payload: Record<string, unknown>): OrderEvent {
  const occurredAt = new Date(payload.occurredAt as string);
  switch (eventType) {
    case OrderPlacedEvent.name:
      return new OrderPlacedEvent(
        payload.orderId as string,
        payload.customerId as string,
        payload.amount as number,
        occurredAt,
      );
    case OrderPaidEvent.name:
      return new OrderPaidEvent(payload.orderId as string, occurredAt);
    case OrderCancelledEvent.name:
      return new OrderCancelledEvent(payload.orderId as string, payload.reason as string, occurredAt);
    default:
      throw new Error(`Unknown event type in stream: ${eventType}`);
  }
}
