import { IQuery, IQueryHandler, QueryHandler } from '@nestjs/cqrs';
import { OrderStatus } from '../../domain/orders/order.aggregate';
import { PrismaService } from '../../infrastructure/prisma.service';

export class GetOrderByIdQuery implements IQuery {
  constructor(public readonly orderId: string) {}
}

export interface OrderDto {
  orderId: string;
  customerId: string;
  amount: number;
  status: OrderStatus;
  version: number;
}

/**
 * Read side of CQS: goes straight to the plain (non-transactional)
 * PrismaService client, never touches an aggregate, never goes anywhere near
 * OutboxCommandHandler — there is nothing for it to save.
 */
@QueryHandler(GetOrderByIdQuery)
export class GetOrderByIdHandler implements IQueryHandler<GetOrderByIdQuery, OrderDto | null> {
  constructor(private readonly prisma: PrismaService) {}

  async execute(query: GetOrderByIdQuery): Promise<OrderDto | null> {
    const record = await this.prisma.order.findUnique({ where: { orderId: query.orderId } });
    if (!record) {
      return null;
    }

    return {
      orderId: record.orderId,
      customerId: record.customerId,
      amount: record.amount.toNumber(),
      status: record.status as OrderStatus,
      version: record.version,
    };
  }
}
