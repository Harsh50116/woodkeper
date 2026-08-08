// Stub consumer: reads the logs topic and prints what arrives. No parsing, no
// Elasticsearch — this exists to prove the read side of the pipeline works.

import { KafkaJS } from "@confluentinc/kafka-javascript";

const BROKERS = (process.env.KAFKA_BROKERS ?? "kafka:9092").split(",");
const TOPIC = process.env.KAFKA_TOPIC ?? "logs";
const GROUP_ID = process.env.KAFKA_GROUP_ID ?? "indexer";

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: BROKERS } });

const consumer = kafka.consumer({
  kafkaJS: {
    groupId: GROUP_ID,
    fromBeginning: true,
    // Offsets are committed manually, only after an event is handled. With a
    // real Elasticsearch write ahead, auto-commit could acknowledge logs that
    // were never actually stored.
    autoCommit: false,
  },
});

const seen = new Map<string, number>();

async function main(): Promise<void> {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC });

  console.log(`[indexer] consuming ${TOPIC} from ${BROKERS.join(",")}`);

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      // A malformed message must not kill the consumer — it would stall the
      // whole partition on one bad record.
      let event: { container?: { name?: string }; message?: unknown };
      try {
        event = JSON.parse(raw);
      } catch {
        console.error(`[indexer] skipping unparseable message at p${partition}@${message.offset}`);
        return;
      }

      const source = event.container?.name ?? "unknown";
      seen.set(source, (seen.get(source) ?? 0) + 1);

      console.log(
        `p${partition}@${message.offset} ${source}: ${String(event.message).slice(0, 120)}`,
      );

      await consumer.commitOffsets([
        { topic: TOPIC, partition, offset: (Number(message.offset) + 1).toString() },
      ]);
    },
  });
}

// Periodic tally, so partition spread and per-source volume are visible without
// reading the firehose.
setInterval(() => {
  if (seen.size > 0) {
    console.log(`[indexer] counts: ${JSON.stringify(Object.fromEntries(seen))}`);
  }
}, 15_000);

async function shutdown(): Promise<void> {
  await consumer.disconnect();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

main().catch((err) => {
  console.error("[indexer] fatal:", err);
  process.exit(1);
});
