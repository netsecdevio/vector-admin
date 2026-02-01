/**
 * NATS Logger for silas-vector workers
 * Reuses the backend logger connection
 */

const { connect, StringCodec } = require("nats");

const COMPONENT = "silas-vector";
const sc = StringCodec();

let nc = null;
let js = null;
let connected = false;

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
    console.log(`[NATS Logger] Workers connected to ${natsUrl}`);
    return true;
  } catch (err) {
    console.error(`[NATS Logger] Connection failed: ${err.message}`);
    return false;
  }
}

async function closeNatsLogger() {
  if (nc) {
    await nc.drain();
    nc = null;
    js = null;
    connected = false;
  }
}

async function publishLog(level, message, metadata = {}) {
  const timestamp = new Date().toISOString();
  const logEntry = {
    timestamp,
    level,
    component: COMPONENT,
    message,
    ...metadata,
  };

  // Console output
  const consoleMethod = level === "error" ? console.error :
                        level === "warn" ? console.warn : console.log;
  consoleMethod(`[${timestamp}] [${level.toUpperCase()}] ${message}`,
    JSON.stringify(metadata.attributes || {}).substring(0, 200));

  if (connected && js) {
    try {
      const subject = `logs.${COMPONENT}.${level}`;
      await js.publish(subject, sc.encode(JSON.stringify(logEntry)));
    } catch (err) {
      console.error(`[NATS Logger] Publish failed: ${err.message}`);
    }
  }
}

const logger = {
  init: initNatsLogger,
  close: closeNatsLogger,
  isConnected: () => connected,

  debug: (msg, meta = {}) => publishLog("debug", msg, meta),
  info: (msg, meta = {}) => publishLog("info", msg, meta),
  warn: (msg, meta = {}) => publishLog("warn", msg, meta),
  error: (msg, meta = {}) => publishLog("error", msg, meta),

  // Job-specific logging
  job: {
    started: (jobName, jobId, data = {}) => {
      return publishLog("info", `Job started: ${jobName}`, {
        attributes: {
          job_name: jobName,
          job_id: jobId,
          event_type: "job.started",
          ...data,
        },
      });
    },

    completed: (jobName, jobId, durationMs, result = {}) => {
      return publishLog("info", `Job completed: ${jobName} in ${durationMs}ms`, {
        attributes: {
          job_name: jobName,
          job_id: jobId,
          event_type: "job.completed",
          duration_ms: durationMs,
          ...result,
        },
      });
    },

    failed: (jobName, jobId, error, durationMs = null) => {
      return publishLog("error", `Job failed: ${jobName} - ${error}`, {
        attributes: {
          job_name: jobName,
          job_id: jobId,
          event_type: "job.failed",
          error: String(error),
          duration_ms: durationMs,
        },
      });
    },

    step: (jobName, stepName, data = {}) => {
      return publishLog("debug", `Job step: ${jobName}/${stepName}`, {
        attributes: {
          job_name: jobName,
          step_name: stepName,
          event_type: "job.step",
          ...data,
        },
      });
    },
  },
};

module.exports = logger;
