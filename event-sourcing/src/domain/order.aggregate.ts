import { OrderCancelledEvent, OrderEvent, OrderPaidEvent, OrderPlacedEvent } from './order-events';

export type OrderStatus = 'Pending' | 'Paid' | 'Cancelled';

export interface OrderSnapshot {
  orderId: string;
  customerId: string;
  amount: number;
  status: OrderStatus;
  version: number;
}

/**
 * The write model. No base class, no decorators, no knowledge of Postgres,
 * KurrentDB, or Nest -- on purpose. Event Sourcing's one rule is "current
 * state is computed by folding events", and the fastest way to prove that
 * rule is actually being followed is to make it impossible for this class to
 * cheat: it has no `save()`, no SQL, nothing to read current state *from*
 * except the events (or a snapshot) handed to it.
 *
 * `apply` is the *only* place that mutates the five fields below. `place`/
 * `pay`/`cancel` validate and construct an event, nothing else -- compare to
 * nest/src/domain/orders/order.aggregate.ts (Block 9), where `pay()` sets
 * `this.status` directly and `apply()` is just notification. That's the
 * actual architectural delta this block is about.
 */
export class Order {
  orderId!: string;
  customerId!: string;
  amount!: number;
  status!: OrderStatus;
  version = 0;

  private uncommitted: OrderEvent[] = [];

  static place(orderId: string, customerId: string, amount: number): Order {
    if (amount <= 0) {
      throw new Error('Order amount must be positive.');
    }
    const order = new Order();
    order.raise(new OrderPlacedEvent(orderId, customerId, amount, new Date()));
    return order;
  }

  pay(): void {
    if (this.status !== 'Pending') {
      throw new Error(`Cannot pay an order in status ${this.status}.`);
    }
    this.raise(new OrderPaidEvent(this.orderId, new Date()));
  }

  cancel(reason: string): void {
    if (this.status === 'Paid') {
      throw new Error('Cannot cancel an order that has already been paid.');
    }
    if (this.status === 'Cancelled') {
      throw new Error('Order is already cancelled.');
    }
    this.raise(new OrderCancelledEvent(this.orderId, reason, new Date()));
  }

  /** Builds an aggregate purely by folding a stream -- never reads a stored "current row". */
  static replay(events: OrderEvent[], fromSnapshot?: OrderSnapshot): Order {
    const order = new Order();
    if (fromSnapshot) Object.assign(order, fromSnapshot);
    for (const event of events) order.apply(event);
    return order;
  }

  toSnapshot(): OrderSnapshot {
    return { orderId: this.orderId, customerId: this.customerId, amount: this.amount, status: this.status, version: this.version };
  }

  getUncommittedEvents(): readonly OrderEvent[] {
    return this.uncommitted;
  }

  clearUncommittedEvents(): void {
    this.uncommitted = [];
  }

  private raise(event: OrderEvent): void {
    this.apply(event);
    this.uncommitted.push(event);
  }

  /** The fold function: event in, new state out. Must stay free of side effects and `new Date()`. */
  private apply(event: OrderEvent): void {
    if (event instanceof OrderPlacedEvent) {
      this.orderId = event.orderId;
      this.customerId = event.customerId;
      this.amount = event.amount;
      this.status = 'Pending';
    } else if (event instanceof OrderPaidEvent) {
      this.status = 'Paid';
    } else if (event instanceof OrderCancelledEvent) {
      this.status = 'Cancelled';
    }
    this.version += 1;
  }
}
