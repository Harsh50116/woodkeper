// Turns raw log lines from each source into one normalized shape.
//
// Sources disagree about everything — nginx writes positional text, postgres
// writes a prefixed sentence, the generator writes JSON. Normalizing here is
// what lets a single query filter across all of them.

export type Level = "DEBUG" | "INFO" | "WARN" | "ERROR" | "FATAL";

export interface LogEntry {
  timestamp: string;
  service: string;
  level: Level;
  message: string;
  traceId?: string;
  metadata: Record<string, unknown>;
}

// 192.168.65.1 - - [07/Aug/2026:00:48:18 +0000] "GET / HTTP/1.1" 200 896 "-" "curl/8.7.1" "-"
const NGINX_ACCESS =
  /^(\S+) \S+ (\S+) \[([^\]]+)\] "(\S+) (\S+) ([^"]*)" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"/;

// 2026/08/07 00:48:18 [error] 29#29: *1 open() "/usr/share/nginx/html/nope" failed
const NGINX_ERROR = /^(\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] (.*)$/;

// 2026-08-07 00:48:18.123 UTC [123] LOG:  statement: select 1;
const POSTGRES =
  /^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \w+) \[(\d+)\](?: \S+)? (\w+):\s+(.*)$/s;

// nginx's own timestamp format: 07/Aug/2026:00:48:18 +0000
function parseNginxTime(raw: string): string {
  const [date, time, offset] = [raw.slice(0, 11), raw.slice(12, 20), raw.slice(21)];
  const parsed = new Date(`${date.replace(/\//g, " ")} ${time} ${offset}`);
  return isNaN(parsed.getTime()) ? new Date().toISOString() : parsed.toISOString();
}

function statusToLevel(status: number): Level {
  if (status >= 500) return "ERROR";
  if (status >= 400) return "WARN";
  return "INFO";
}

// Postgres severities don't map 1:1 to ours — PANIC and FATAL both mean the
// process is dying, and NOTICE/DETAIL are noise at INFO level.
function postgresLevel(severity: string): Level {
  switch (severity.toUpperCase()) {
    case "DEBUG":
    case "DEBUG1":
    case "DEBUG2":
      return "DEBUG";
    case "WARNING":
      return "WARN";
    case "ERROR":
      return "ERROR";
    case "FATAL":
    case "PANIC":
      return "FATAL";
    default:
      return "INFO";
  }
}

function parseNginx(line: string, fallbackTime: string): LogEntry {
  const access = NGINX_ACCESS.exec(line);
  if (access) {
    const [, clientIp, user, time, method, path, protocol, status, bytes, referer, agent] = access;
    const statusCode = Number(status);
    return {
      timestamp: parseNginxTime(time),
      service: "nginx",
      level: statusToLevel(statusCode),
      message: `${method} ${path} ${statusCode}`,
      metadata: {
        clientIp,
        user: user === "-" ? undefined : user,
        method,
        path,
        protocol,
        status: statusCode,
        bytes: bytes === "-" ? 0 : Number(bytes),
        referer: referer === "-" ? undefined : referer,
        userAgent: agent,
      },
    };
  }

  const error = NGINX_ERROR.exec(line);
  if (error) {
    const [, time, severity, message] = error;
    return {
      timestamp: new Date(time.replace(/\//g, "-").replace(" ", "T") + "Z").toISOString(),
      service: "nginx",
      level: severity === "error" || severity === "crit" ? "ERROR" : "WARN",
      message,
      metadata: { severity },
    };
  }

  return { timestamp: fallbackTime, service: "nginx", level: "INFO", message: line, metadata: {} };
}

function parsePostgres(line: string, fallbackTime: string): LogEntry {
  const match = POSTGRES.exec(line);
  if (!match) {
    return {
      timestamp: fallbackTime,
      service: "postgres",
      level: "INFO",
      message: line,
      metadata: {},
    };
  }

  const [, time, pid, severity, body] = match;
  const metadata: Record<string, unknown> = { pid: Number(pid), severity };

  // "duration: 0.123 ms  statement: select 1;" — the statement itself is the
  // useful part, the duration is worth having as a number.
  const duration = /^duration: ([\d.]+) ms/.exec(body);
  if (duration) metadata.durationMs = Number(duration[1]);

  const statement = /statement: (.*)$/s.exec(body);
  if (statement) metadata.statement = statement[1].trim();

  return {
    timestamp: new Date(time.replace(" UTC", "Z").replace(" ", "T")).toISOString(),
    service: "postgres",
    level: postgresLevel(severity),
    message: body.trim(),
    metadata,
  };
}

// The generator already emits our target shape, so this is mostly validation.
function parseGenerator(line: string, fallbackTime: string): LogEntry {
  try {
    const parsed = JSON.parse(line);
    return {
      timestamp: parsed.timestamp ?? fallbackTime,
      service: parsed.service ?? "unknown",
      level: (parsed.level ?? "INFO") as Level,
      message: parsed.message ?? "",
      traceId: parsed.traceId,
      metadata: parsed.metadata ?? {},
    };
  } catch {
    // Startup banners and burst notices go to stderr as plain text.
    return {
      timestamp: fallbackTime,
      service: "generator",
      level: "INFO",
      message: line,
      metadata: { unparsed: true },
    };
  }
}

export function parse(source: string, line: string, fallbackTime: string): LogEntry {
  switch (source) {
    case "nginx":
      return parseNginx(line, fallbackTime);
    case "postgres":
      return parsePostgres(line, fallbackTime);
    case "generator":
      return parseGenerator(line, fallbackTime);
    default:
      return { timestamp: fallbackTime, service: source, level: "INFO", message: line, metadata: {} };
  }
}
