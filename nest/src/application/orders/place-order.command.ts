import { CommandHandler, ICommand } from '@nestjs/cqrs';
import { Prisma } from '@prisma/client';
import { Order } from '../../domain/orders/order.aggregate';
import { OutboxCommandHandler } from '../../infrastructure/outbox-command.handler';
import { PrismaService } from '../../infrastructure/prisma.service';

export class PlaceOrderCommand implements ICommand {
  constructor(
    public readonly orderId: string,
    public readonly customerId: string,
    public readonly amount: number,
  ) {}
}

/**
 * This handler is the entire "developer contract" from the practice task:
 * create the aggregate, call a domain method (Order.place already raises the
 * event), write its own row. There is no event-persistence code here —
 * OutboxCommandHandler.execute does that after this method returns.
 */
@CommandHandler(PlaceOrderCommand)
export class PlaceOrderHandler extends OutboxCommandHandler<PlaceOrderCommand, string> {
  constructor(prisma: PrismaService) {
    super(prisma);
  }

  protected async handle(command: PlaceOrderCommand, tx: Prisma.TransactionClient) {
    const order = Order.place(command.orderId, command.customerId, command.amount);

    await tx.order.create({
      data: {
        orderId: order.orderId,
        customerId: order.customerId,
        amount: order.amount,
        status: order.status,
        version: order.version,
        createdAt: order.createdAt,
        updatedAt: order.updatedAt,
      },
    });

    return { result: order.orderId, aggregates: [order] };
  }
}
