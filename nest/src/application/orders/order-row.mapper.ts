import { Order as OrderRecord } from '@prisma/client';
import { OrderRow, OrderStatus } from '../../domain/orders/order.aggregate';

export function toOrderRow(record: OrderRecord): OrderRow {
  return {
    orderId: record.orderId,
    customerId: record.customerId,
    amount: record.amount.toNumber(),
    status: record.status as OrderStatus,
    version: record.version,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  };
}
