require("dotenv").config();
const { Inngest } = require("inngest");
const natsLogger = require("./logger");

/**
 * Inngest middleware for NATS-based job logging
 * Automatically logs job start, completion, and failure for all worker functions
 */
const natsLoggingMiddleware = {
  name: "NATS Job Logging",

  init() {
    return {
      onFunctionRun({ fn, ctx }) {
        const jobName = fn.name || ctx.event?.name || "unknown";
        const jobId = ctx.runId || ctx.event?.id || "unknown";
        const startTime = Date.now();

        // Log job started
        natsLogger.job.started(jobName, jobId, {
          event_name: ctx.event?.name,
          event_data: JSON.stringify(ctx.event?.data || {}).substring(0, 500),
        });

        return {
          transformOutput({ result, step }) {
            const duration = Date.now() - startTime;

            if (result.error) {
              // Job failed
              natsLogger.job.failed(jobName, jobId, result.error, duration);
            } else {
              // Job completed
              natsLogger.job.completed(jobName, jobId, duration, {
                result_preview: JSON.stringify(result.data || {}).substring(
                  0,
                  200
                ),
              });
            }

            return result;
          },
        };
      },
    };
  },
};

const InngestClient = new Inngest({
  name: "VDMS Background Workers",
  eventKey: process.env.INNGEST_EVENT_KEY || "background_workers",
  middleware: [natsLoggingMiddleware],
});

/**
 * Initialize NATS logger for workers
 * Call this at startup before processing jobs
 */
async function initWorkerLogging() {
  return await natsLogger.init();
}

/**
 * Close NATS logger connection
 * Call this during shutdown
 */
async function closeWorkerLogging() {
  return await natsLogger.close();
}

module.exports = {
  InngestClient,
  initWorkerLogging,
  closeWorkerLogging,
  natsLogger,
};
