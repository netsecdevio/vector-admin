/**
 * NATS Logger for silas-vector
 *
 * Publishes structured logs to NATS JetStream for centralized logging.
 * Falls back to console logging if NATS is unavailable.
 */

const { connect, StringCodec } = require("nats");

const COMPONENT = "silas-vector";
const sc = StringCodec();

let nc = null;
let js = null;
let connected = false;

/**
 * Initialize NATS connection
 */
async function initNatsLogger() {
  const natsUrl = process.env.NATS_URL;

  if (!natsUrl) {
    console.log("[NATS Logger] NATS_URL not set, using console logging only");
    return false;
  }

  try {
    nc = await connect({ servers: natsUrl });
    js = nc.jetstream();
    connected = true;
    console.log(`[NATS Logger] Connected to ${natsUrl}`);

    // Handle connection close
    nc.closed().then(() => {
      connected = false;
      console.log("[NATS Logger] Connection closed");
    });

    return true;
  } catch (err) {
    console.error(`[NATS Logger] Connection failed: ${err.message}`);
    connected = false;
    return false;
  }
}

/**
 * Close NATS connection
 */
async function closeNatsLogger() {
  if (nc) {
    await nc.drain();
    nc = null;
    js = null;
    connected = false;
  }
}

/**
 * Publish a log entry to NATS
 */
async function publishLog(level, message, metadata = {}) {
  const timestamp = new Date().toISOString();
  const logEntry = {
    timestamp,
    level,
    component: COMPONENT,
    message,
    ...metadata,
  };

  // Always log to console
  const consoleMethod = level === "error" ? console.error :
                        level === "warn" ? console.warn :
                        level === "debug" ? console.debug : console.log;
  consoleMethod(`[${timestamp}] [${level.toUpperCase()}] ${message}`, metadata.attributes || "");

  // Publish to NATS if connected
  if (connected && js) {
    try {
      const subject = `logs.${COMPONENT}.${level}`;
      await js.publish(subject, sc.encode(JSON.stringify(logEntry)));
    } catch (err) {
      // Don't log recursively on failure
      console.error(`[NATS Logger] Failed to publish: ${err.message}`);
    }
  }
}

// Convenience methods
const logger = {
  init: initNatsLogger,
  close: closeNatsLogger,
  isConnected: () => connected,

  trace: (message, metadata = {}) => publishLog("trace", message, metadata),
  debug: (message, metadata = {}) => publishLog("debug", message, metadata),
  info: (message, metadata = {}) => publishLog("info", message, metadata),
  warn: (message, metadata = {}) => publishLog("warn", message, metadata),
  error: (message, metadata = {}) => publishLog("error", message, metadata),
  fatal: (message, metadata = {}) => publishLog("fatal", message, metadata),

  // Log with custom attributes
  log: (level, message, attributes = {}) => {
    return publishLog(level, message, { attributes });
  },

  // Log request/response
  request: (method, path, statusCode, durationMs, metadata = {}) => {
    const level = statusCode >= 500 ? "error" : statusCode >= 400 ? "warn" : "info";
    return publishLog(level, `${method} ${path} ${statusCode} ${durationMs}ms`, {
      attributes: {
        method,
        path,
        status_code: statusCode,
        duration_ms: durationMs,
        ...metadata,
      },
    });
  },

  // Log database operation
  database: (operation, target, success, durationMs, error = null) => {
    const level = success ? "debug" : "error";
    const message = success
      ? `DB ${operation} on ${target} completed in ${durationMs}ms`
      : `DB ${operation} on ${target} failed: ${error}`;
    return publishLog(level, message, {
      attributes: {
        operation,
        target,
        success,
        duration_ms: durationMs,
        error,
      },
    });
  },

  // Log connector operation
  connector: (connectorType, operation, success, metadata = {}) => {
    const level = success ? "info" : "error";
    const message = success
      ? `Connector ${connectorType}: ${operation} succeeded`
      : `Connector ${connectorType}: ${operation} failed`;
    return publishLog(level, message, {
      attributes: {
        connector_type: connectorType,
        operation,
        success,
        ...metadata,
      },
    });
  },
};

module.exports = logger;
