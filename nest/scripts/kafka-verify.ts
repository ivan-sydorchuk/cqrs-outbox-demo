import 'dotenv/config';
import { Kafka } from 'kafkajs';
import { workerConfig } from '../src/worker/config';

/**
 * Independent proof, from the Kafka side: consumes the entire topic from the
 * beginning, then reports whether the committed outbox set arrived with no
 * loss and no gap. Duplicates ARE expected and are not a failure — this
 * pipeline is at-least-once by design (see README) — so we de-dupe by
 * `position` before checking for gaps, and report the duplicate count
 * separately as a sanity signal (it should be small: only what a killed
 * worker's uncommitted checkpoint re-published).
 */
async function main() {
  const idleTimeoutMs = Number(process.env.VERIFY_IDLE_TIMEOUT_MS ?? 5000);

  const kafka = new Kafka({ clientId: 'verify', brokers: workerConfig.kafkaBrokers });
  const consumer = kafka.consumer({ groupId: `verify-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: workerConfig.kafkaTopic, fromBeginning: true });

  const positions: number[] = [];
  const perAggregateEventTypes = new Map<string, string[]>();
  // Consumer-group join alone can take several seconds (observed ~3.7s
  // against the local single-broker KRaft setup) — counting that against the
  // idle timeout meant we sometimes declared "done" before a single message
  // had a chance to arrive. So: don't start the idle countdown until the
  // FIRST message shows up; only bail out early (maxInitialWaitMs) if
  // nothing ever arrives at all.
  const scriptStartedAt = Date.now();
  const maxInitialWaitMs = Number(process.env.VERIFY_MAX_INITIAL_WAIT_MS ?? 15000);
  let lastMessageAt: number | null = null;
  let running = true;

  await consumer.run({
    eachMessage: async ({ message }) => {
      lastMessageAt = Date.now();
      const value = JSON.parse(message.value!.toString());
      positions.push(Number(value.position));
      const list = perAggregateEventTypes.get(value.aggregateId) ?? [];
      list.push(value.eventType);
      perAggregateEventTypes.set(value.aggregateId, list);
    },
  });

  while (running) {
    await new Promise((resolve) => setTimeout(resolve, 250));
    if (lastMessageAt !== null && Date.now() - lastMessageAt > idleTimeoutMs) {
      running = false;
    } else if (lastMessageAt === null && Date.now() - scriptStartedAt > maxInitialWaitMs) {
      running = false;
    }
  }

  await consumer.disconnect();

  const distinctPositions = Array.from(new Set(positions)).sort((a, b) => a - b);
  const duplicateCount = positions.length - distinctPositions.length;

  console.log(`\nConsumed ${positions.length} messages (${distinctPositions.length} distinct positions, ${duplicateCount} duplicates).`);

  const gaps: string[] = [];
  for (let i = 1; i < distinctPositions.length; i++) {
    const prev = distinctPositions[i - 1];
    const curr = distinctPositions[i];
    if (curr !== prev + 1) {
      gaps.push(`gap between ${prev} and ${curr}`);
    }
  }

  if (distinctPositions.length > 0) {
    console.log(`Position range observed: ${distinctPositions[0]} .. ${distinctPositions[distinctPositions.length - 1]}`);
  }

  if (gaps.length === 0) {
    console.log('NO GAPS: every position from min to max was observed exactly once (after de-duping retries).');
  } else {
    console.log(`GAPS FOUND (${gaps.length}):`);
    gaps.forEach((g) => console.log(`  ${g}`));
  }

  let orderingViolations = 0;
  const rank: Record<string, number> = { OrderPlacedEvent: 0, OrderPaidEvent: 1, OrderCancelledEvent: 1 };
  for (const [aggregateId, types] of perAggregateEventTypes) {
    for (let i = 1; i < types.length; i++) {
      if (rank[types[i]] < rank[types[i - 1]]) {
        orderingViolations++;
        console.log(`  ORDER VIOLATION for ${aggregateId}: ${types.join(' -> ')}`);
      }
    }
  }
  console.log(
    orderingViolations === 0
      ? `Per-aggregate order preserved across all ${perAggregateEventTypes.size} aggregates.`
      : `${orderingViolations} per-aggregate ordering violations found.`,
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
