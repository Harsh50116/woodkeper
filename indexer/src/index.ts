// Consumes raw log events from Kafka, parses them into a common shape, and
// bulk-writes them to Elasticsearch.
//
// Offsets are committed only after Elasticsearch acknowledges the write. If ES
// is slow or down, nothing is committed, messages stay in Kafka, and the backlog
// waits there instead of being lost.

import { KafkaJS } from "@confluentinc/kafka-javascript";
import { parse } from "./parsers.ts";
import { bulkIndex, ensureTemplate, type Indexable } from "./elasticsearch.ts";

const BROKERS = (process.env.KAFKA_BROKERS ?? "kafka:9092").split(",");
const TOPIC = process.env.KAFKA_TOPIC ?? "logs";
const GROUP_ID = process.env.KAFKA_GROUP_ID ?? "indexer";
const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? 500);

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: BROKERS } });

const consumer = kafka.consumer({
  kafkaJS: {
    groupId: GROUP_ID,
    fromBeginning: true,
    autoCommit: false,
  },
});

const indexed = new Map<string, number>();
let failures = 0;

async function main(): Promise<void> {
  await ensureTemplate();
  console.log("[indexer] index template applied");

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC });
  console.log(`[indexer] consuming ${TOPIC} from ${BROKERS.join(",")}`);

  await consumer.run({
    // eachBatch rather than eachMessage: bulk writes are the whole point, and
    // per-message handling would mean one HTTP round trip per log line.
    eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, isRunning }) => {
      const pending: Indexable[] = [];

      const flush = async (): Promise<void> => {
        if (pending.length === 0) return;
        await bulkIndex(pending);
        // Only mark offsets resolved once the write actually succeeded.
        for (const item of pending) resolveOffset(item.offset);
        await commitOffsetsIfNecessary();
        pending.length = 0;
      };

      for (const message of batch.messages) {
        if (!isRunning()) break;

        const raw = message.value?.toString();
        if (!raw) continue;

        let event: { container?: { name?: string }; message?: string; "@timestamp"?: string };
        try {
          event = JSON.parse(raw);
        } catch {
          // A malformed envelope is skipped rather than retried forever — it
          // would block the partition permanently.
          resolveOffset(message.offset);
          failures += 1;
          continue;
        }

        const source = event.container?.name ?? "unknown";
        const fallbackTime = event["@timestamp"] ?? new Date().toISOString();
        const entry = parse(source, event.message ?? "", fallbackTime);

        pending.push({ entry, partition: batch.partition, offset: message.offset });
        indexed.set(source, (indexed.get(source) ?? 0) + 1);

        if (pending.length >= BATCH_SIZE) await flush();
      }

      await flush();
    },
  });
}

setInterval(() => {
  if (indexed.size > 0) {
    const counts = JSON.stringify(Object.fromEntries(indexed));
    console.log(`[indexer] indexed: ${counts}${failures > 0 ? ` failures=${failures}` : ""}`);
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
