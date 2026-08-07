import { CommandHandler, ICommand } from '@nestjs/cqrs';
import { Prisma } from '@prisma/client';
import { Order } from '../../domain/orders/order.aggregate';
import { OutboxCommandHandler } from '../../infrastructure/outbox-command.handler';
import { PrismaService } from '../../infrastructure/prisma.service';
import { toOrderRow } from './order-row.mapper';

export class PayOrderCommand implements ICommand {
  constructor(public readonly orderId: string) {}
}

@CommandHandler(PayOrderCommand)
export class PayOrderHandler extends OutboxCommandHandler<PayOrderCommand, void> {
  constructor(prisma: PrismaService) {
    super(prisma);
  }

  protected async handle(command: PayOrderCommand, tx: Prisma.TransactionClient) {
    const record = await tx.order.findUnique({ where: { orderId: command.orderId } });
    if (!record) {
      throw new Error(`Order ${command.orderId} not found.`);
    }

    const order = Order.hydrate(toOrderRow(record));
    order.pay();

    await tx.order.update({
      where: { orderId: order.orderId },
      data: { status: order.status, version: order.version, updatedAt: order.updatedAt },
    });

    return { result: undefined, aggregates: [order] };
  }
}
