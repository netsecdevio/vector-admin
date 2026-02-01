process.env.NODE_ENV === "development"
  ? require("dotenv").config({ path: `.env.${process.env.NODE_ENV}` })
  : require("dotenv").config();

const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const path = require("path");
const { systemEndpoints } = require("./endpoints/system");
const { systemInit } = require("./utils/boot");
const { authenticationEndpoints } = require("./endpoints/auth");
const { v1Endpoints } = require("./endpoints/v1");
const { setupDebugger } = require("./utils/debug");
const { Telemetry } = require("./models/telemetry");
const logger = require("./utils/logger");
const app = express();

// Request logging middleware
app.use((req, res, next) => {
  const start = Date.now();
  res.on("finish", () => {
    const duration = Date.now() - start;
    // Only log API requests, skip static assets
    if (req.path.startsWith("/api")) {
      logger.request(req.method, req.path, res.statusCode, duration);
    }
  });
  next();
});
const apiRouter = express.Router();

app.use(cors({ origin: true }));
app.use(
  bodyParser.text({
    limit: "10GB",
  })
);
app.use(
  bodyParser.json({
    limit: "10GB",
  })
);
app.use(
  bodyParser.urlencoded({
    limit: "10GB",
    extended: true,
  })
);

app.use("/api", apiRouter);
authenticationEndpoints(apiRouter);
systemEndpoints(apiRouter);
v1Endpoints(apiRouter);

if (process.env.NODE_ENV !== "development") {
  app.use(
    express.static(path.resolve(__dirname, "public"), { extensions: ["js"] })
  );

  app.use("/", function (_, response) {
    response.sendFile(path.join(__dirname, "public", "index.html"));
  });

  app.get("/robots.txt", function (_, response) {
    response.type("text/plain");
    response.send("User-agent: *\nDisallow: /").end();
  });
}

app.all("*", function (_, response) {
  response.sendStatus(404);
});

app
  .listen(process.env.SERVER_PORT || 3001, async () => {
    // Initialize NATS logger
    await logger.init();

    await systemInit();
    setupDebugger(apiRouter);

    logger.info("Server started", {
      attributes: {
        port: process.env.SERVER_PORT || 3001,
        node_env: process.env.NODE_ENV,
      },
    });
    console.log(
      `Backend server listening on port ${process.env.SERVER_PORT || 3001}`
    );
  })
  .on("error", function (err) {
    logger.error("Server error", { attributes: { error: err.message } });
    process.once("SIGUSR2", async function () {
      Telemetry.flush();
      await logger.close();
      process.kill(process.pid, "SIGUSR2");
    });
    process.on("SIGINT", async function () {
      Telemetry.flush();
      await logger.close();
      process.kill(process.pid, "SIGINT");
    });
  });
