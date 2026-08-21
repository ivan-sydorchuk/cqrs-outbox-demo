import { Kafka, Producer } from 'kafkajs';
import { workerConfig } from './config';

export function createKafkaProducer(): Producer {
  const kafka = new Kafka({
    clientId: workerConfig.workerId,
    brokers: workerConfig.kafkaBrokers,
  });

  // idempotent: true dedupes retries of the SAME produce request at the
  // broker (a network blip resending one batch won't create duplicates from
  // that alone). It does NOT make the whole outbox->Kafka pipeline
  // exactly-once — a crash between a successful publish and the checkpoint
  // update still redelivers that batch on restart. See README: at-least-once
  // is the actual guarantee; consumers must dedupe by event position.
  return kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
}
