/**
 * ClickHouse connector for vector-admin
 *
 * Note: ClickHouse is primarily an OLAP database, not a vector DB.
 * This connector provides browsing capabilities for Silas time-series data
 * (jobs, metrics, logs) stored in ClickHouse.
 */

class ClickHouse {
  constructor(connector) {
    this.name = "clickhouse";
    this.config = this.setConfig(connector);
  }

  setConfig(config) {
    var { type, settings } = config;
    if (typeof settings === "string") settings = JSON.parse(settings);
    return { type, settings };
  }

  async connect() {
    const { createClient } = require("@clickhouse/client");
    const { settings } = this.config;

    const client = createClient({
      host: `http://${settings.host}:${settings.port || 8123}`,
      username: settings.username || "default",
      password: settings.password || "",
      database: settings.database || "default",
    });

    // Verify connection
    const result = await client.ping();
    if (!result.success) {
      throw new Error("ClickHouse::Connection failed - ping unsuccessful");
    }

    return { client };
  }

  async heartbeat() {
    try {
      const { client } = await this.connect();
      const result = await client.ping();
      await client.close();
      return { result: result.success, error: null };
    } catch (e) {
      return { result: false, error: e.message };
    }
  }

  async totalIndicies() {
    try {
      const { client } = await this.connect();
      const { settings } = this.config;
      const database = settings.database || "default";

      const result = await client.query({
        query: `SELECT sum(rows) as total FROM system.parts WHERE database = '${database}' AND active = 1`,
        format: "JSONEachRow",
      });

      const data = await result.json();
      await client.close();
      return { result: parseInt(data[0]?.total || 0), error: null };
    } catch (e) {
      return { result: 0, error: e.message };
    }
  }

  // Tables === namespaces for ClickHouse to normalize interfaces
  async collections() {
    return await this.namespaces();
  }

  async namespaces() {
    try {
      const { client } = await this.connect();
      const { settings } = this.config;
      const database = settings.database || "default";

      // Get all tables with row counts
      const result = await client.query({
        query: `
          SELECT
            name,
            total_rows,
            total_bytes,
            engine
          FROM system.tables
          WHERE database = '${database}'
          ORDER BY name
        `,
        format: "JSONEachRow",
      });

      const tables = await result.json();
      await client.close();

      return tables.map((t) => ({
        name: t.name,
        count: parseInt(t.total_rows || 0),
        metadata: {
          engine: t.engine,
          bytes: parseInt(t.total_bytes || 0),
        },
      }));
    } catch (e) {
      console.error("namespaces error:", e.message);
      return [];
    }
  }

  async namespace(name = null) {
    if (!name) throw new Error("No namespace (table) value provided.");

    try {
      const { client } = await this.connect();
      const { settings } = this.config;
      const database = settings.database || "default";

      // Get table info
      const tableResult = await client.query({
        query: `
          SELECT
            name,
            total_rows,
            total_bytes,
            engine,
            create_table_query
          FROM system.tables
          WHERE database = '${database}' AND name = '${name}'
        `,
        format: "JSONEachRow",
      });

      const tableInfo = await tableResult.json();
      if (!tableInfo.length) {
        await client.close();
        return null;
      }

      // Get column info
      const columnsResult = await client.query({
        query: `
          SELECT
            name,
            type,
            default_kind,
            default_expression
          FROM system.columns
          WHERE database = '${database}' AND table = '${name}'
        `,
        format: "JSONEachRow",
      });

      const columns = await columnsResult.json();
      await client.close();

      return {
        name,
        count: parseInt(tableInfo[0]?.total_rows || 0),
        metadata: {
          engine: tableInfo[0]?.engine,
          bytes: parseInt(tableInfo[0]?.total_bytes || 0),
          columns: columns,
          createQuery: tableInfo[0]?.create_table_query,
        },
      };
    } catch (e) {
      console.error("namespace error:", e.message);
      return null;
    }
  }

  async namespaceExists(_client, name = null) {
    if (!name) throw new Error("No namespace (table) value provided.");
    try {
      const { client } = await this.connect();
      const { settings } = this.config;
      const database = settings.database || "default";

      const result = await client.query({
        query: `SELECT 1 FROM system.tables WHERE database = '${database}' AND name = '${name}'`,
        format: "JSONEachRow",
      });

      const data = await result.json();
      await client.close();
      return data.length > 0;
    } catch (e) {
      return false;
    }
  }

  async rawGet(tableName, pageSize = 10, offset = 0) {
    try {
      const { client } = await this.connect();
      const { settings } = this.config;
      const database = settings.database || "default";

      const result = await client.query({
        query: `SELECT * FROM ${database}.${tableName} LIMIT ${pageSize} OFFSET ${offset}`,
        format: "JSONEachRow",
      });

      const data = await result.json();
      await client.close();

      return {
        ids: data.map((r, i) => r.id || r._id || `row_${offset + i}`),
        data: data,
        error: null,
      };
    } catch (e) {
      console.error("rawGet error:", e.message);
      return { ids: [], data: [], error: e.message };
    }
  }

  // ClickHouse is not a vector DB, so these methods are simplified/stubbed
  async processDocument(
    tableName,
    documentData,
    embedderApiKey,
    dbDocument
  ) {
    // ClickHouse doesn't process documents the same way vector DBs do
    // This is a stub to maintain interface compatibility
    return {
      success: false,
      message: "ClickHouse is not a vector database - document processing not supported"
    };
  }

  async similarityResponse(namespace, queryVector, topK = 4) {
    // ClickHouse can support vector similarity with specialized functions
    // but this would require specific table schema setup
    return {
      vectorIds: [],
      contextTexts: [],
      sourceDocuments: [],
      scores: [],
    };
  }

  async getMetadata(namespace = "", ids = []) {
    if (!ids.length) return [];

    try {
      const { client } = await this.connect();
      const { settings } = this.config;
      const database = settings.database || "default";

      const idList = ids.map((id) => `'${id}'`).join(",");
      const result = await client.query({
        query: `SELECT * FROM ${database}.${namespace} WHERE id IN (${idList})`,
        format: "JSONEachRow",
      });

      const data = await result.json();
      await client.close();
      return data;
    } catch (e) {
      console.error("getMetadata error:", e.message);
      return [];
    }
  }

  async deleteVectors(namespace, ids = []) {
    try {
      const { client } = await this.connect();
      const { settings } = this.config;
      const database = settings.database || "default";

      const idList = ids.map((id) => `'${id}'`).join(",");
      await client.command({
        query: `ALTER TABLE ${database}.${namespace} DELETE WHERE id IN (${idList})`,
      });

      await client.close();
      return { success: true };
    } catch (e) {
      console.error("deleteVectors error:", e.message);
      return { success: false, error: e.message };
    }
  }

  // ClickHouse-specific query method
  async executeQuery(query, format = "JSONEachRow") {
    try {
      const { client } = await this.connect();
      const result = await client.query({ query, format });
      const data = await result.json();
      await client.close();
      return { data, error: null };
    } catch (e) {
      console.error("executeQuery error:", e.message);
      return { data: [], error: e.message };
    }
  }
}

module.exports.ClickHouse = ClickHouse;
