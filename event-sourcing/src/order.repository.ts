import { Inject, Injectable } from '@nestjs/common';
import { Order } from './domain/order.aggregate';
import { deserializeOrderEvent } from './domain/order-events';
import { EVENT_STORE, EventStore, NewEvent } from './event-store/event-store';
import { SNAPSHOT_STORE, SnapshotStore } from './snapshot/snapshot-store';
import { OrderSummaryProjection } from './projection/order-summary.projection';

/**
 * The only class that knows both "Order" and "EventStore" exist. Everything
 * it does reduces to two operations, and neither one branches on which
 * EventStore implementation is bound -- that's the "zero changes to
 * domain/application code" requirement made literal: this file is identical
 * whether EVENT_STORE resolves to Postgres or KurrentDB.
 */
@Injectable()
export class OrderRepository {
  private readonly snapshotEvery = Number(process.env.SNAPSHOT_EVERY ?? 0);

  constructor(
    @Inject(EVENT_STORE) private readonly eventStore: EventStore,
    @Inject(SNAPSHOT_STORE) private readonly snapshots: SnapshotStore,
    private readonly projection: OrderSummaryProjection,
  ) {}

  /**
   * `asOfVersion`, when given, is the temporal query: replaying the same
   * stream but stopping partway through gives the aggregate's state as of
   * that version, with no separate "history" code path. Snapshots are
   * skipped for a temporal read so the result is never accidentally wrong
   * for a version older than the latest snapshot.
   */
  async load(streamId: string, asOfVersion?: number): Promise<Order | null> {
    const snapshot = asOfVersion === undefined ? await this.snapshots.load(streamId) : null;
    // `fromVersion: snapshot.version` is what makes the snapshot an actual
    // read-size saving: the store is asked for only the events folded on
    // top of it, not the full stream filtered client-side afterwards.
    const storedEvents = await this.eventStore.readStream(streamId, {
      fromVersion: snapshot?.version,
      toVersion: asOfVersion,
    });

    if (!snapshot && storedEvents.length === 0) return null;

    const domainEvents = storedEvents.map((e) => deserializeOrderEvent(e.eventType, e.payload));
    return Order.replay(domainEvents, snapshot ?? undefined);
  }

  /** Appends whatever the aggregate raised since it was loaded/created, with its own expected version. */
  async save(order: Order): Promise<void> {
    const uncommitted = order.getUncommittedEvents();
    if (uncommitted.length === 0) return;

    const expectedVersion = order.version - uncommitted.length;
    const newEvents: NewEvent[] = uncommitted.map((event) => ({
      eventType: event.constructor.name,
      payload: JSON.parse(JSON.stringify(event)) as Record<string, unknown>,
      occurredAt: event.occurredAt,
    }));

    await this.eventStore.append(order.orderId, expectedVersion, newEvents);
    order.clearUncommittedEvents();

    if (this.snapshotEvery > 0 && order.version % this.snapshotEvery === 0) {
      await this.snapshots.save(order.toSnapshot());
    }

    // See order-summary.projection.ts: inline-after-append is a demo
    // simplification, not the pattern to copy into a real system.
    await this.projection.apply(order);
  }
}
