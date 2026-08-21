export const workerConfig = {
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9094').split(','),
  kafkaTopic: process.env.KAFKA_TOPIC ?? 'order-events',
  // Number of independently-claimable shards. This is the actual unit of
  // horizontal scaling: each shard is its own event_subscription row, so up
  // to shardCount worker instances can hold a distinct shard's lock and
  // publish in parallel without ever touching each other's rows. See
  // sql/block10-worker-extension.sql for how the rows are seeded.
  shardCount: Number(process.env.WORKER_SHARD_COUNT ?? 2),
  batchSize: Number(process.env.WORKER_BATCH_SIZE ?? 20),
  pollIntervalMs: Number(process.env.WORKER_POLL_INTERVAL_MS ?? 500),
  workerId: process.env.WORKER_ID ?? `worker-${process.pid}`,
};

export function shardName(shardIndex: number): string {
  return `kafka-order-events-${shardIndex}`;
}
