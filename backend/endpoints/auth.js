process.env.NODE_ENV === "development"
  ? require("dotenv").config({ path: `.env.${process.env.NODE_ENV}` })
  : require("dotenv").config();
const { SystemSettings } = require("../models/systemSettings");
const { Telemetry } = require("../models/telemetry");
const { User } = require("../models/user");
const { reqBody, makeJWT } = require("../utils/http");
const bcrypt = require("bcrypt");
const logger = require("../utils/logger");

function authenticationEndpoints(app) {
  if (!app) return;

  app.get("/auth/auto-onboard", async (_, response) => {
    try {
      const completeSetup = (await User.count({ role: "admin" })) > 0;
      if (completeSetup) {
        logger.debug("Auto-onboard check - setup complete", {
          attributes: { action: "auto_onboard", result: "already_complete" },
        });
        response.status(200).json({ completed: true });
        return;
      }

      const onboardingUser = await User.get({ role: "root" });
      if (!onboardingUser) {
        logger.debug("Auto-onboard check - no root user", {
          attributes: { action: "auto_onboard", result: "no_root_user" },
        });
        response.status(200).json({ completed: true });
        return;
      }

      logger.info("Auto-onboard successful", {
        attributes: {
          action: "auto_onboard",
          result: "success",
          user_id: onboardingUser.id,
        },
      });
      await Telemetry.sendTelemetry("onboarding_complete"); // Have to send here since we have no other hooks.
      response.status(200).json({
        valid: true,
        user: onboardingUser,
        token: makeJWT(
          { id: onboardingUser.id, email: onboardingUser.email },
          "1hr"
        ),
        message: null,
      });
    } catch (e) {
      logger.error("Auto-onboard error", {
        attributes: { action: "auto_onboard", error: e.message },
      });
      console.log(e.message, e);
      response.sendStatus(500).end();
    }
  });

  app.post("/auth/login", async (request, response) => {
    try {
      const { email, password } = reqBody(request);
      if (!email || !password) {
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message: "[002] No email or password provided.",
        });
        return;
      }

      if (email === process.env.SYS_EMAIL) {
        const completeSetup = (await User.count({ role: "admin" })) > 0;
        if (completeSetup) {
          response.status(200).json({
            user: null,
            valid: false,
            token: null,
            message: "[004] Invalid login credentials.",
          });
          return;
        }
      }

      const existingUser = await User.get({ email: email });
      if (!existingUser) {
        logger.warn("Login failed - user not found", {
          attributes: { email, reason: "user_not_found" },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message: "[001] Invalid login credentials.",
        });
        return;
      }

      if (!bcrypt.compareSync(password, existingUser.password)) {
        logger.warn("Login failed - invalid password", {
          attributes: { email, reason: "invalid_password" },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message: "[002] Invalid login credentials.",
        });
        return;
      }

      logger.info("Login successful", {
        attributes: { email, user_id: existingUser.id },
      });
      await Telemetry.sendTelemetry("login_event");
      response.status(200).json({
        valid: true,
        user: existingUser,
        token: makeJWT(
          { id: existingUser.id, email: existingUser.email },
          "30d"
        ),
        message: null,
      });
      return;
    } catch (e) {
      logger.error("Login error", { attributes: { error: e.message } });
      console.log(e.message, e);
      response.sendStatus(500).end();
    }
  });

  app.post("/auth/create-account", async (request, response) => {
    try {
      const { email, password } = reqBody(request);
      if (!email || !password) {
        logger.warn("Account creation failed - missing credentials", {
          attributes: { action: "create_account", reason: "missing_credentials" },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message: "[002] No email or password provided.",
        });
        return;
      }

      const adminCount = await User.count({ role: "admin" });
      if (adminCount === 0) {
        logger.warn("Account creation blocked - system not setup", {
          attributes: { action: "create_account", email, reason: "system_not_setup" },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message:
            "[000] System setup has not been completed - account creation disabled.",
        });
        return;
      }

      const existingUser = await User.get({ email });
      if (!!existingUser) {
        logger.warn("Account creation failed - email exists", {
          attributes: { action: "create_account", email, reason: "email_exists" },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message: "[001] Account already exists by this email - use another.",
        });
        return;
      }

      const allowingAccounts = await SystemSettings.get({
        label: "allow_account_creation",
      });
      if (
        !!allowingAccounts &&
        allowingAccounts.value !== null &&
        allowingAccounts.value === "false"
      ) {
        logger.warn("Account creation blocked - disabled by admin", {
          attributes: { action: "create_account", email, reason: "creation_disabled" },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message: "[003] Access denied.",
        });
        return;
      }

      const domainRestriction = await SystemSettings.get({
        label: "account_creation_domain_scope",
      });
      if (domainRestriction && domainRestriction.value) {
        const emailDomain = email.substring(email.lastIndexOf("@") + 1);
        if (emailDomain !== domainRestriction.value) {
          logger.warn("Account creation blocked - domain mismatch", {
            attributes: {
              action: "create_account",
              email,
              reason: "domain_restricted",
              allowed_domain: domainRestriction.value,
            },
          });
          response.status(200).json({
            user: null,
            valid: false,
            token: null,
            message: "[003] Invalid account creation values.",
          });
          return;
        }
      }

      const { user, message } = await User.create({ email, password });
      if (!user) {
        logger.error("Account creation failed", {
          attributes: { action: "create_account", email, reason: "creation_error", error: message },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message,
        });
        return;
      }

      logger.info("Account created successfully", {
        attributes: { action: "create_account", email, user_id: user.id },
      });
      await User.addToAllOrgs(user.id);
      await Telemetry.sendTelemetry("login_event");
      response.status(200).json({
        user,
        valid: true,
        token: makeJWT({ id: user.id, email: user.email }, "30d"),
        message: null,
      });
      return;
    } catch (e) {
      logger.error("Account creation error", {
        attributes: { action: "create_account", error: e.message },
      });
      console.log(e.message, e);
      response.sendStatus(500).end();
    }
  });

  app.post("/auth/transfer-root", async (request, response) => {
    try {
      const { email, password } = reqBody(request);
      if (!email || !password) {
        logger.warn("Transfer root failed - missing credentials", {
          attributes: { action: "transfer_root", reason: "missing_credentials" },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message: "[002] No email or password provided.",
        });
        return;
      }

      const adminCount = await User.count({ role: "admin" });
      if (adminCount > 0) {
        logger.warn("Transfer root blocked - already configured", {
          attributes: { action: "transfer_root", email, reason: "already_configured" },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message:
            "[000] System setup has already been completed - you cannot do this again.",
        });
        return;
      }

      const existingUser = await User.get({ email });
      if (!!existingUser) {
        logger.warn("Transfer root failed - email exists", {
          attributes: { action: "transfer_root", email, reason: "email_exists" },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message: "[001] Account already exists by this email - use another.",
        });
        return;
      }

      const { user, message } = await User.create({
        email,
        password,
        role: "admin",
      });
      if (!user) {
        logger.error("Transfer root failed - user creation error", {
          attributes: { action: "transfer_root", email, reason: "creation_error", error: message },
        });
        response.status(200).json({
          user: null,
          valid: false,
          token: null,
          message,
        });
        return;
      }

      logger.info("Transfer root successful - admin created", {
        attributes: { action: "transfer_root", email, user_id: user.id, role: "admin" },
      });
      response.status(200).json({
        user,
        valid: true,
        token: makeJWT({ id: user.id, email: user.email }, "30d"),
        message: null,
      });
      return;
    } catch (e) {
      logger.error("Transfer root error", {
        attributes: { action: "transfer_root", error: e.message },
      });
      console.log(e.message, e);
      response.sendStatus(500).end();
    }
  });
}

module.exports = { authenticationEndpoints };
