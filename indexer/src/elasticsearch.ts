// Elasticsearch client, index template, and bulk writing.

import { Client } from "@elastic/elasticsearch";
import type { LogEntry } from "./parsers.ts";

const NODE = process.env.ES_NODE ?? "http://elasticsearch:9200";
const INDEX_PREFIX = process.env.ES_INDEX_PREFIX ?? "logs";

export const client = new Client({ node: NODE });

// Without an explicit template Elasticsearch guesses field types from whatever
// arrives first. It typically maps timestamps as text, which silently breaks
// time-range queries and sorting — the classic ELK mistake.
const TEMPLATE = {
  index_patterns: [`${INDEX_PREFIX}-*`],
  template: {
    settings: {
      number_of_shards: 1,
      // Single node, so a replica could never be allocated and the cluster
      // would sit permanently yellow.
      number_of_replicas: 0,
    },
    mappings: {
      properties: {
        timestamp: { type: "date" },
        // keyword: exact-match filters and aggregations. text would tokenize
        // "payment-service" into "payment" and "service".
        service: { type: "keyword" },
        source: { type: "keyword" },
        level: { type: "keyword" },
        traceId: { type: "keyword" },
        // text: analyzed for full-text search.
        message: { type: "text" },
        raw: { type: "text" },
        // Shape differs per source, so let ES infer — but keep strings as
        // keywords rather than analyzed text.
        metadata: { type: "object", dynamic: true },
      },
    },
  },
};

export async function ensureTemplate(): Promise<void> {
  await client.indices.putIndexTemplate({
    name: `${INDEX_PREFIX}-template`,
    ...TEMPLATE,
  });
}

// Daily indices: retention becomes "delete an index" instead of a mass delete
// query, and time-range searches skip whole indices they don't need.
function indexNameFor(timestamp: string): string {
  const date = new Date(timestamp);
  const day = (isNaN(date.getTime()) ? new Date() : date).toISOString().slice(0, 10);
  return `${INDEX_PREFIX}-${day.replace(/-/g, ".")}`;
}

export interface Indexable {
  entry: LogEntry;
  partition: number;
  offset: string;
}

// Throws if any document failed, so the caller can decline to commit offsets
// and let Kafka redeliver.
export async function bulkIndex(items: Indexable[]): Promise<void> {
  if (items.length === 0) return;

  const operations = items.flatMap(({ entry, partition, offset }) => [
    {
      index: {
        _index: indexNameFor(entry.timestamp),
        // Deterministic ID: a redelivered message overwrites its own document
        // instead of creating a duplicate.
        _id: `${INDEX_PREFIX}-${partition}-${offset}`,
      },
    },
    entry,
  ]);

  const response = await client.bulk({ operations, refresh: false });

  if (response.errors) {
    const failed = response.items.filter((item) => item.index?.error);
    const reason = failed[0]?.index?.error?.reason ?? "unknown";
    throw new Error(`${failed.length}/${items.length} documents rejected: ${reason}`);
  }
}
