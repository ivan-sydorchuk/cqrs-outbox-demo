/**
 * The one interface the domain/application layer is allowed to depend on.
 * Both `postgres-event-store.ts` and `kurrentdb-event-store.ts` implement
 * this and nothing else leaks through -- `order.repository.ts` never
 * imports `pg` or `@kurrent/kurrentdb-client` directly.
 */

export interface NewEvent {
  eventType: string;
  payload: Record<string, unknown>;
  occurredAt: Date;
}

export interface StoredEvent extends NewEvent {
  streamId: string;
  /** 1-based: the first event ever appended to a stream has version 1. */
  version: number;
}

export interface EventStore {
  /**
   * `expectedVersion` is how many events the caller believes already exist
   * in the stream (0 == "this stream must not exist yet"). Both
   * implementations must reject the whole batch with {@link ConcurrencyError}
   * if the stream's actual version has moved past that -- this is the
   * optimistic-concurrency check that prevents lost updates (Block 8).
   */
  append(streamId: string, expectedVersion: number, events: NewEvent[]): Promise<void>;

  /**
   * Reads a stream in version order, optionally bounded on either side.
   * `toVersion` stops after that version (inclusive) -- this is what makes
   * a temporal query ("state as of version N") just a narrower
   * `readStream` call, not a separate code path. `fromVersion` is
   * exclusive (events with version > fromVersion) -- this is what makes a
   * snapshot an actual read-size optimization rather than a replay-CPU-only
   * one: `OrderRepository.load` passes the snapshot's own version so the
   * store never has to fetch events the snapshot already folded.
   */
  readStream(streamId: string, options?: ReadStreamOptions): Promise<StoredEvent[]>;
}

export interface ReadStreamOptions {
  fromVersion?: number;
  toVersion?: number;
}

export class ConcurrencyError extends Error {
  constructor(streamId: string, expectedVersion: number, actualVersion?: number) {
    super(
      `Concurrency conflict on stream "${streamId}": expected version ${expectedVersion}` +
        (actualVersion === undefined ? '.' : `, actual version is ${actualVersion}.`),
    );
    this.name = 'ConcurrencyError';
  }
}

/** DI token -- the Nest module binds this to whichever implementation EVENT_STORE env var picks. */
export const EVENT_STORE = Symbol('EVENT_STORE');
