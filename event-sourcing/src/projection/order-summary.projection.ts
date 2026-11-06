import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool } from 'pg';
import { Order, OrderStatus } from '../domain/order.aggregate';

export interface OrderSummaryRow {
  orderId: string;
  customerId: string;
  amount: number;
  status: OrderStatus;
  version: number;
  updatedAt: Date;
}

/**
 * The read model the mentor notes keep pointing at: "you can't SELECT across
 * aggregates from the event log, build a projection." `order_projection` is
 * a plain table kept in sync by folding events -- the demo updates it
 * right after each `OrderRepository.save()`, which is honest for a
 * single-process demo but is the one simplification worth flagging: a real
 * system updates projections asynchronously (a subscription, or the Block 10
 * outbox worker), never inline in the command's own transaction, or you've
 * just rebuilt the "second source of truth" problem one layer up.
 */
@Injectable()
export class OrderSummaryProjection implements OnModuleDestroy {
  private readonly pool = new Pool({ connectionString: process.env.EVENT_SOURCING_DATABASE_URL });

  async apply(order: Order): Promise<void> {
    await this.pool.query(
      `INSERT INTO order_projection (order_id, customer_id, amount, status, version, updated_at)
       VALUES ($1, $2, $3, $4, $5, now())
       ON CONFLICT (order_id) DO UPDATE SET
         customer_id = $2, amount = $3, status = $4, version = $5, updated_at = now()`,
      [order.orderId, order.customerId, order.amount, order.status, order.version],
    );
  }

  async findById(orderId: string): Promise<OrderSummaryRow | null> {
    const { rows } = await this.pool.query(
      `SELECT order_id, customer_id, amount, status, version, updated_at FROM order_projection WHERE order_id = $1`,
      [orderId],
    );
    return rows[0] ? toRow(rows[0]) : null;
  }

  /** The actual payoff: a query no single stream replay could answer. */
  async listByStatus(status: OrderStatus): Promise<OrderSummaryRow[]> {
    const { rows } = await this.pool.query(
      `SELECT order_id, customer_id, amount, status, version, updated_at
       FROM order_projection WHERE status = $1 ORDER BY updated_at ASC`,
      [status],
    );
    return rows.map(toRow);
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }
}

function toRow(row: {
  order_id: string;
  customer_id: string;
  amount: string;
  status: OrderStatus;
  version: number;
  updated_at: Date;
}): OrderSummaryRow {
  return {
    orderId: row.order_id,
    customerId: row.customer_id,
    amount: Number(row.amount),
    status: row.status,
    version: row.version,
    updatedAt: row.updated_at,
  };
}
