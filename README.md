# woodkeper

A centralized logging pipeline, built to learn distributed systems hands-on.

Logs from real containers and a synthetic generator ship through Kafka, get parsed and enriched by a TypeScript indexer, land in Elasticsearch, and are searched in Kibana. Error spikes fire a Slack alert.

## Architecture

```
[Nginx | Postgres | generator] → [Filebeat] → [Kafka] → [Indexer] → [Elasticsearch] → [Kibana]
                                                            └→ [Slack]
```

## Stack

| Layer | Choice |
|-------|--------|
| Agent | Filebeat |
| Broker | Kafka (single broker to start) |
| Indexer | TypeScript / Node, `@confluentinc/kafka-javascript` |
| Storage | Elasticsearch (single node, daily indices) |
| UI | Kibana |
| Alerting | Slack webhook, fired in-stream by the indexer |

## Design notes

- **No API service.** Filebeat is the only ingress, Kibana the only read path.
- **Ingestion is async and eventually consistent.** Seconds of indexing lag is fine; blocking a producer is not.
- **Kafka is the point.** It buffers incident-time spikes and replays if the indexer or ES goes down.
- **At-least-once delivery.** Offsets are committed only after a successful bulk write; duplicates are handled by document ID.

Mixed log formats (Nginx, Postgres, JSON) are deliberate — the indexer has to parse and normalize them. The generator emits synthetic trace IDs so cross-service tracing is demonstrable.

## Status

Design complete, implementation not started.

Full scope and locked decisions: `temp/project_scope.txt` (untracked).
