import { Module } from '@nestjs/common';
import { CqrsModule } from '@nestjs/cqrs';
import { PrismaService } from './infrastructure/prisma.service';
import { PlaceOrderHandler } from './application/orders/place-order.command';
import { PayOrderHandler } from './application/orders/pay-order.command';
import { CancelOrderHandler } from './application/orders/cancel-order.command';
import { GetOrderByIdHandler } from './application/orders/get-order-by-id.query';

@Module({
  imports: [CqrsModule],
  providers: [PrismaService, PlaceOrderHandler, PayOrderHandler, CancelOrderHandler, GetOrderByIdHandler],
  exports: [PrismaService],
})
export class OrdersModule {}
