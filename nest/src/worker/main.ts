import 'dotenv/config';
import { PrismaClient } from '@prisma/client';
import { workerConfig } from './config';
import { createKafkaProducer } from './kafka';
import { pollShard } from './outbox-relay';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  const prisma = new PrismaClient();
  const producer = createKafkaProducer();
  await producer.connect();

  console.log(`[${workerConfig.workerId}] started, shards=${workerConfig.shardCount}, topic=${workerConfig.kafkaTopic}`);

  let stopping = false;
  const stop = () => {
    console.log(`[${workerConfig.workerId}] shutdown signal received, stopping after current tick...`);
    stopping = true;
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);

  while (!stopping) {
    let publishedThisTick = 0;

    for (let shard = 0; shard < workerConfig.shardCount; shard++) {
      // A single tick's failure (a deadlock with a concurrent admin query, a
      // dropped connection, a Kafka retry exhausted) must not take the whole
      // worker process down — that would turn a transient blip into an
      // outage. The transaction itself never partially commits (Prisma rolls
      // it back), so it's always safe to just log and retry next tick.
      try {
        const result = await pollShard(prisma, producer, shard);
        if (result.published > 0) {
          console.log(`[${workerConfig.workerId}] shard=${shard} published=${result.published}`);
          publishedThisTick += result.published;
        }
      } catch (error) {
        console.error(`[${workerConfig.workerId}] shard=${shard} poll failed, will retry next tick:`, error);
      }
    }

    if (publishedThisTick === 0) {
      await sleep(workerConfig.pollIntervalMs);
    }
  }

  await producer.disconnect();
  await prisma.$disconnect();
  console.log(`[${workerConfig.workerId}] stopped cleanly.`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
