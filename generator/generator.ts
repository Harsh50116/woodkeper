// Synthetic log generator. Emits JSON lines to stdout, one per log entry.
// Each "request" produces several correlated entries sharing a traceId, so a
// single trace can be followed across services.

type Level = "DEBUG" | "INFO" | "WARN" | "ERROR";

interface LogEntry {
  timestamp: string;
  service: string;
  level: Level;
  message: string;
  traceId: string;
  metadata: { host: string; region: string; podId: string };
}

const TRACES_PER_SEC = Number(process.env.TRACES_PER_SEC ?? 5);
const BURST_INTERVAL_MS = Number(process.env.BURST_INTERVAL_MS ?? 120_000);
const BURST_DURATION_MS = Number(process.env.BURST_DURATION_MS ?? 10_000);
const BASELINE_ERROR_RATE = 0.02;
const BURST_ERROR_RATE = 0.4;

// Realistic request paths through the system. Every request enters at the gateway.
const FLOWS = [
  ["api-gateway", "auth-service", "payment-service", "notification-service"],
  ["api-gateway", "auth-service", "scheduling-service"],
  ["api-gateway", "auth-service", "inventory-service", "payment-service"],
  ["api-gateway", "auth-service", "user-service"],
];

const MESSAGES: Record<string, Record<Level, string[]>> = {
  "api-gateway": {
    DEBUG: ["matched route /v1/checkout", "request headers parsed"],
    INFO: ["routed request to auth-service", "request completed in 142ms"],
    WARN: ["upstream latency above threshold (850ms)", "rate limit at 80% for client"],
    ERROR: ["upstream returned 502", "request timed out after 30s"],
  },
  "auth-service": {
    DEBUG: ["decoding bearer token", "loading permission set"],
    INFO: ["token validated for user", "session refreshed"],
    WARN: ["token expires in under 60s", "repeated failed login for account"],
    ERROR: ["token signature verification failed", "identity provider unreachable"],
  },
  "payment-service": {
    DEBUG: ["building charge request", "idempotency key resolved"],
    INFO: ["charge authorized", "refund processed"],
    WARN: ["retrying charge after soft decline", "payment gateway slow (1.2s)"],
    ERROR: ["payment gateway timeout", "card authorization declined by issuer"],
  },
  "scheduling-service": {
    DEBUG: ["expanding recurrence rule", "checking calendar availability"],
    INFO: ["appointment booked", "reminder scheduled"],
    WARN: ["double booking detected, resolving", "slot released late"],
    ERROR: ["failed to persist appointment", "calendar sync failed"],
  },
  "notification-service": {
    DEBUG: ["rendering email template", "resolving recipient preferences"],
    INFO: ["email queued for delivery", "push notification sent"],
    WARN: ["delivery retried once", "recipient has notifications muted"],
    ERROR: ["smtp connection refused", "push token rejected by provider"],
  },
  "inventory-service": {
    DEBUG: ["reading stock level from cache", "acquiring reservation lock"],
    INFO: ["stock reserved", "reservation released"],
    WARN: ["stock below reorder threshold", "reservation lock contended"],
    ERROR: ["insufficient stock for reservation", "inventory database deadlock"],
  },
  "user-service": {
    DEBUG: ["loading profile from cache", "hydrating preferences"],
    INFO: ["profile retrieved", "profile updated"],
    WARN: ["profile cache miss", "stale profile served"],
    ERROR: ["profile lookup failed", "database connection pool exhausted"],
  },
};

const REGIONS = ["us-east-1", "us-west-2", "eu-west-1"];

const pick = <T>(items: T[]): T => items[Math.floor(Math.random() * items.length)];

let bursting = false;

function pickLevel(): Level {
  const errorRate = bursting ? BURST_ERROR_RATE : BASELINE_ERROR_RATE;
  const r = Math.random();
  if (r < errorRate) return "ERROR";
  if (r < errorRate + 0.08) return "WARN";
  if (r < errorRate + 0.23) return "DEBUG";
  return "INFO";
}

function emit(service: string, level: Level, traceId: string): void {
  const entry: LogEntry = {
    timestamp: new Date().toISOString(),
    service,
    level,
    message: pick(MESSAGES[service][level]),
    traceId,
    metadata: {
      host: `${service}-${Math.floor(Math.random() * 4) + 1}`,
      region: pick(REGIONS),
      podId: `pod-${Math.random().toString(36).slice(2, 8)}`,
    },
  };
  console.log(JSON.stringify(entry));
}

// One trace walks its flow hop by hop, with a small delay between services so
// timestamps reflect the real ordering of a distributed request.
function emitTrace(): void {
  const traceId = crypto.randomUUID();
  const flow = pick(FLOWS);

  flow.forEach((service, hop) => {
    setTimeout(() => emit(service, pickLevel(), traceId), hop * (20 + Math.random() * 60));
  });
}

setInterval(emitTrace, 1000 / TRACES_PER_SEC);

setInterval(() => {
  bursting = true;
  console.error(`[generator] error burst started (${BURST_DURATION_MS}ms)`);
  setTimeout(() => {
    bursting = false;
    console.error("[generator] error burst ended");
  }, BURST_DURATION_MS);
}, BURST_INTERVAL_MS);

console.error(
  `[generator] ${TRACES_PER_SEC} traces/sec, burst every ${BURST_INTERVAL_MS / 1000}s`,
);
