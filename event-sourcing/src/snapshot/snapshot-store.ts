import { OrderSnapshot } from '../domain/order.aggregate';

/**
 * Optional by design: delete every row a {@link SnapshotStore} ever wrote and
 * `OrderRepository.load` must still return the exact same aggregate -- just
 * slower, because it falls back to replaying the full stream. A snapshot is
 * a cache of a fold, never a second source of truth.
 */
export interface SnapshotStore {
  load(streamId: string): Promise<OrderSnapshot | null>;
  save(snapshot: OrderSnapshot): Promise<void>;
}

export const SNAPSHOT_STORE = Symbol('SNAPSHOT_STORE');
