import { Module } from '@nestjs/common';
import { EVENT_STORE } from './event-store/event-store';
import { PostgresEventStore } from './event-store/postgres-event-store';
import { KurrentDbEventStore } from './event-store/kurrentdb-event-store';
import { SNAPSHOT_STORE } from './snapshot/snapshot-store';
import { PostgresSnapshotStore } from './snapshot/postgres-snapshot-store';
import { OrderSummaryProjection } from './projection/order-summary.projection';
import { OrderRepository } from './order.repository';

/**
 * The entire storage swap lives in this one `useClass` line. `EVENT_STORE`
 * (env var) picks `postgres` or `kurrentdb`; `OrderRepository` and `Order`
 * import neither class directly, so toggling it is the whole migration.
 */
@Module({
  providers: [
    {
      provide: EVENT_STORE,
      useClass: process.env.EVENT_STORE === 'kurrentdb' ? KurrentDbEventStore : PostgresEventStore,
    },
    { provide: SNAPSHOT_STORE, useClass: PostgresSnapshotStore },
    OrderSummaryProjection,
    OrderRepository,
  ],
  exports: [OrderRepository, OrderSummaryProjection],
})
export class AppModule {}
