import { Injectable, OnModuleDestroy } from '@nestjs/common';
import {
  FORWARDS,
  jsonEvent,
  KurrentDBClient,
  NO_STREAM,
  START,
  StreamNotFoundError,
  WrongExpectedVersionError,
} from '@kurrent/kurrentdb-client';
import { v4 as uuid } from 'uuid';
import { ConcurrencyError, EventStore, NewEvent, ReadStreamOptions, StoredEvent } from './event-store';

/**
 * Same `EventStore` contract as `PostgresEventStore`, satisfied by a
 * purpose-built event store instead of a table. KurrentDB gives streams,
 * per-stream revisions, and append-with-expected-revision natively, so this
 * class is mostly a translation layer:
 *
 * - our `expectedVersion` (1-based count of events already appended, 0 for
 *   a brand-new stream) <-> KurrentDB's `streamState` (0-based revision of
 *   the last written event, or the `NO_STREAM` sentinel)
 * - KurrentDB's `WrongExpectedVersionError` <-> our storage-agnostic
 *   {@link ConcurrencyError}, so `order.repository.ts` never has to know
 *   which store it's talking to.
 */
@Injectable()
export class KurrentDbEventStore implements EventStore, OnModuleDestroy {
  private readonly client = KurrentDBClient.connectionString(
    process.env.KURRENTDB_CONNECTION_STRING ?? 'kurrentdb://localhost:2113?tls=false',
  );

  async append(streamId: string, expectedVersion: number, events: NewEvent[]): Promise<void> {
    if (events.length === 0) return;

    const streamState = expectedVersion === 0 ? NO_STREAM : BigInt(expectedVersion - 1);
    const toAppend = events.map((event) =>
      jsonEvent({
        id: uuid(),
        type: event.eventType,
        data: event.payload,
      }),
    );

    try {
      await this.client.appendToStream(streamName(streamId), toAppend, { streamState });
    } catch (error) {
      if (error instanceof WrongExpectedVersionError) {
        const actual = error.actualState;
        const actualVersion = actual === NO_STREAM ? 0 : Number(actual) + 1;
        throw new ConcurrencyError(streamId, expectedVersion, actualVersion);
      }
      throw error;
    }
  }

  async readStream(streamId: string, options: ReadStreamOptions = {}): Promise<StoredEvent[]> {
    const { fromVersion, toVersion } = options;
    const events = this.client.readStream(streamName(streamId), {
      direction: FORWARDS,
      // fromVersion is exclusive (events strictly after it); KurrentDB's
      // fromRevision is the revision to start reading FROM (inclusive), and
      // revision N (0-based) is exactly our 1-based version N+1 -- so
      // "start after version V" and "start at revision V" are the same cut.
      fromRevision: fromVersion === undefined ? START : BigInt(fromVersion),
    });

    const result: StoredEvent[] = [];
    try {
      for await (const resolved of events) {
        const event = resolved.event;
        if (!event) continue; // link events only -- never written here, but the type allows it

        const version = Number(event.revision) + 1;
        if (toVersion !== undefined && version > toVersion) break;

        const payload = event.data as Record<string, unknown>;
        result.push({
          streamId,
          version,
          eventType: event.type,
          payload,
          // The store's own revision has no occurred-at we rely on -- the
          // event's own `occurredAt` field (inside payload) is what the
          // aggregate was built with, so that's what we hand back too.
          occurredAt: new Date(payload.occurredAt as string),
        });
      }
    } catch (error) {
      if (error instanceof StreamNotFoundError) return []; // same as Postgres: unknown stream == no events
      throw error;
    }
    return result;
  }
  async onModuleDestroy(): Promise<void> {
    await this.client.dispose();
  }
}

function streamName(streamId: string): string {
  return `order-${streamId}`;
}
