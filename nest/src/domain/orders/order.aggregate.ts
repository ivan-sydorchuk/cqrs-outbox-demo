import { AggregateRoot } from '@nestjs/cqrs';
import { OrderPlacedEvent } from './events/order-placed.event';
import { OrderPaidEvent } from './events/order-paid.event';
import { OrderCancelledEvent } from './events/order-cancelled.event';

export type OrderStatus = 'Pending' | 'Paid' | 'Cancelled';

export interface OrderRow {
  orderId: string;
  customerId: string;
  amount: number;
  status: OrderStatus;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * The write-model aggregate. `AggregateRoot` (from @nestjs/cqrs) is the
 * marker/base class the task asks for — it already exposes
 * getUncommittedEvents()/commit(), so we don't need to hand-roll one. Every
 * state transition below calls the inherited `apply(event)` — that is the
 * entire developer contract for producing an event. Nothing here knows the
 * outbox exists, and `apply` never triggers NestJS's own EventBus unless the
 * aggregate is merged with an EventPublisher context, which we deliberately
 * never do — this task only persists events, it doesn't publish them.
 */
export class Order extends AggregateRoot implements OrderRow {
  orderId!: string;
  customerId!: string;
  amount!: number;
  status!: OrderStatus;
  version!: number;
  createdAt!: Date;
  updatedAt!: Date;

  static place(orderId: string, customerId: string, amount: number): Order {
    if (amount <= 0) {
      throw new Error('Order amount must be positive.');
    }

    const now = new Date();
    const order = new Order();
    order.orderId = orderId;
    order.customerId = customerId;
    order.amount = amount;
    order.status = 'Pending';
    order.version = 1;
    order.createdAt = now;
    order.updatedAt = now;

    order.apply(new OrderPlacedEvent(orderId, customerId, amount, now));
    return order;
  }

  /** Rebuilds an aggregate from a persisted row so a command handler can call a domain method on it. */
  static hydrate(row: OrderRow): Order {
    const order = new Order();
    Object.assign(order, row);
    return order;
  }

  pay(): void {
    if (this.status !== 'Pending') {
      throw new Error(`Cannot pay an order in status ${this.status}.`);
    }

    this.status = 'Paid';
    this.touch();
    this.apply(new OrderPaidEvent(this.orderId, this.updatedAt));
  }

  cancel(reason: string): void {
    if (this.status === 'Paid') {
      throw new Error('Cannot cancel an order that has already been paid.');
    }
    if (this.status === 'Cancelled') {
      throw new Error('Order is already cancelled.');
    }

    this.status = 'Cancelled';
    this.touch();
    this.apply(new OrderCancelledEvent(this.orderId, reason, this.updatedAt));
  }

  private touch(): void {
    this.version += 1;
    this.updatedAt = new Date();
  }
}
