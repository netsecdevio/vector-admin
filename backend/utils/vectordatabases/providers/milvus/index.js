const { RecursiveCharacterTextSplitter } = require("langchain/text_splitter");
const { OpenAi } = require("../../../openAi");
const { v4 } = require("uuid");
const { DocumentVectors } = require("../../../../models/documentVectors");
const { toChunks } = require("../../utils");
const { storeVectorResult } = require("../../../storage");
const { WorkspaceDocument } = require("../../../../models/workspaceDocument");

class Milvus {
  constructor(connector) {
    this.name = "milvus";
    this.config = this.setConfig(connector);
  }

  setConfig(config) {
    var { type, settings } = config;
    if (typeof settings === "string") settings = JSON.parse(settings);
    return { type, settings };
  }

  distanceToScore(distance = null) {
    if (distance === null || typeof distance !== "number") return 0.0;
    // Milvus returns similarity scores (higher is better for IP/COSINE)
    // For L2, lower is better so we invert
    if (distance >= 1.0) return 1;
    if (distance <= 0) return 0;
    return distance;
  }

  async connect() {
    const { MilvusClient } = require("@zilliz/milvus2-sdk-node");
    const { type, settings } = this.config;

    if (type !== "milvus")
      throw new Error("Milvus::Invalid Not a Milvus connector instance.");

    const address = settings.port
      ? `${settings.host}:${settings.port}`
      : settings.host;

    const clientConfig = { address };

    // Add authentication if provided
    if (settings.username && settings.password) {
      clientConfig.username = settings.username;
      clientConfig.password = settings.password;
    }

    // Add token auth if provided (for Zilliz Cloud)
    if (settings.token) {
      clientConfig.token = settings.token;
    }

    const client = new MilvusClient(clientConfig);

    // Verify connection
    const health = await client.checkHealth();
    if (!health.isHealthy)
      throw new Error("Milvus::Connection failed - cluster is not healthy");

    return { client };
  }

  async heartbeat() {
    try {
      const { client } = await this.connect();
      const health = await client.checkHealth();
      return { result: health.isHealthy, error: null };
    } catch (e) {
      return { result: false, error: e.message };
    }
  }

  async totalIndicies() {
    try {
      const { client } = await this.connect();
      const { settings } = this.config;
      const dbName = settings.database || "default";

      // List all collections
      const collections = await client.listCollections();
      let totalVectors = 0;

      for (const collectionName of collections.collection_names || []) {
        try {
          const stats = await client.getCollectionStatistics({
            collection_name: collectionName,
          });
          totalVectors += parseInt(stats.data?.row_count || 0);
        } catch (e) {
          console.error(`Failed to get stats for ${collectionName}:`, e.message);
        }
      }

      return { result: totalVectors, error: null };
    } catch (e) {
      return { result: 0, error: e.message };
    }
  }

  async indexDimensions(namespace) {
    try {
      const { client } = await this.connect();
      const info = await client.describeCollection({
        collection_name: namespace,
      });

      // Find the vector field and get its dimension
      for (const field of info.schema?.fields || []) {
        if (field.data_type === "FloatVector" || field.data_type === 101) {
          const dimParam = field.type_params?.find((p) => p.key === "dim");
          if (dimParam) return parseInt(dimParam.value);
        }
      }

      // Default to OpenAI's dimension
      return 1536;
    } catch (e) {
      console.error("indexDimensions error:", e.message);
      return 1536;
    }
  }

  // Collections === namespaces for Milvus to normalize interfaces
  async collections() {
    return await this.namespaces();
  }

  async namespaces() {
    try {
      const { client } = await this.connect();
      const result = await client.listCollections();
      const collections = [];

      for (const name of result.collection_names || []) {
        try {
          const stats = await client.getCollectionStatistics({
            collection_name: name,
          });
          const info = await client.describeCollection({
            collection_name: name,
          });

          collections.push({
            name,
            count: parseInt(stats.data?.row_count || 0),
            metadata: {
              description: info.schema?.description || "",
              fields: info.schema?.fields?.length || 0,
            },
          });
        } catch (e) {
          collections.push({ name, count: 0, metadata: {} });
        }
      }

      return collections;
    } catch (e) {
      console.error("namespaces error:", e.message);
      return [];
    }
  }

  async namespace(name = null) {
    if (!name) throw new Error("No namespace value provided.");

    try {
      const { client } = await this.connect();
      const info = await client.describeCollection({
        collection_name: name,
      });
      const stats = await client.getCollectionStatistics({
        collection_name: name,
      });

      return {
        name,
        count: parseInt(stats.data?.row_count || 0),
        schema: info.schema,
        metadata: {
          description: info.schema?.description || "",
          fields: info.schema?.fields || [],
        },
      };
    } catch (e) {
      console.error("namespace error:", e.message);
      return null;
    }
  }

  async namespaceExists(_client, name = null) {
    if (!name) throw new Error("No namespace value provided.");
    try {
      const { client } = await this.connect();
      const result = await client.hasCollection({ collection_name: name });
      return result.value;
    } catch (e) {
      return false;
    }
  }

  async rawGet(collectionName, pageSize = 10, offset = 0) {
    try {
      const { client } = await this.connect();

      // Ensure collection is loaded
      await client.loadCollection({ collection_name: collectionName });

      // Query with pagination
      const result = await client.query({
        collection_name: collectionName,
        output_fields: ["*"],
        limit: pageSize,
        offset: offset,
      });

      return {
        ids: result.data?.map((r) => r.id || r._id) || [],
        data: result.data || [],
        error: null,
      };
    } catch (e) {
      console.error("rawGet error:", e.message);
      return { ids: [], data: [], error: e.message };
    }
  }

  async processDocument(
    collectionName,
    documentData,
    embedderApiKey,
    dbDocument
  ) {
    try {
      const openai = new OpenAi(embedderApiKey);
      const { pageContent, id, ...metadata } = documentData;
      const textSplitter = new RecursiveCharacterTextSplitter({
        chunkSize: 1000,
        chunkOverlap: 20,
      });
      const textChunks = await textSplitter.splitText(pageContent);

      console.log("Chunks created from document:", textChunks.length);
      const documentVectors = [];
      const cacheInfo = [];
      const vectors = [];
      const vectorValues = await openai.embedTextChunks(textChunks);

      if (!!vectorValues && vectorValues.length > 0) {
        for (const [i, vector] of vectorValues.entries()) {
          const vectorRecord = {
            id: v4(),
            values: vector,
            metadata: { ...metadata, text: textChunks[i] },
          };

          vectors.push(vectorRecord);
          documentVectors.push({
            docId: id,
            vectorId: vectorRecord.id,
            documentId: dbDocument.id,
            workspaceId: dbDocument.workspace_id,
            organizationId: dbDocument.organization_id,
          });
          cacheInfo.push({
            vectorDbId: vectorRecord.id,
            values: vector,
            metadata: vectorRecord.metadata,
          });
        }
      } else {
        console.error(
          "Could not use OpenAI to embed document chunk! This document will not be recorded."
        );
        return { success: false, message: "Failed to generate embeddings" };
      }

      const { client } = await this.connect();

      // Ensure collection is loaded
      await client.loadCollection({ collection_name: collectionName });

      // Insert vectors in batches
      for (const chunk of toChunks(vectors, 500)) {
        const insertData = chunk.map((v) => ({
          id: v.id,
          vector: v.values,
          text: v.metadata.text || "",
          metadata: JSON.stringify(v.metadata),
        }));

        await client.insert({
          collection_name: collectionName,
          data: insertData,
        });
      }

      await DocumentVectors.createMany(documentVectors);
      await storeVectorResult(
        cacheInfo,
        WorkspaceDocument.vectorFilename(dbDocument)
      );
      return { success: true, message: null };
    } catch (e) {
      console.error("processDocument error:", e.message);
      return { success: false, message: e.message };
    }
  }

  async similarityResponse(namespace, queryVector, topK = 4) {
    try {
      const { client } = await this.connect();

      // Ensure collection is loaded
      await client.loadCollection({ collection_name: namespace });

      const result = await client.search({
        collection_name: namespace,
        vector: queryVector,
        limit: topK,
        output_fields: ["*"],
      });

      const response = {
        vectorIds: [],
        contextTexts: [],
        sourceDocuments: [],
        scores: [],
      };

      for (const hit of result.results || []) {
        response.vectorIds.push(hit.id);
        response.contextTexts.push(hit.text || "");
        response.scores.push(this.distanceToScore(hit.score));

        let metadata = {};
        try {
          metadata = hit.metadata ? JSON.parse(hit.metadata) : {};
        } catch (e) {
          metadata = { text: hit.text };
        }
        response.sourceDocuments.push(metadata);
      }

      return response;
    } catch (e) {
      console.error("similarityResponse error:", e.message);
      return {
        vectorIds: [],
        contextTexts: [],
        sourceDocuments: [],
        scores: [],
      };
    }
  }

  async getMetadata(namespace = "", vectorIds = []) {
    try {
      const { client } = await this.connect();

      // Ensure collection is loaded
      await client.loadCollection({ collection_name: namespace });

      const result = await client.query({
        collection_name: namespace,
        filter: `id in [${vectorIds.map((id) => `"${id}"`).join(",")}]`,
        output_fields: ["*"],
      });

      return (result.data || []).map((item) => {
        let metadata = {};
        try {
          metadata = item.metadata ? JSON.parse(item.metadata) : {};
        } catch (e) {
          metadata = {};
        }
        return {
          ...metadata,
          vectorId: item.id,
          text: item.text || metadata.text || "",
        };
      });
    } catch (e) {
      console.error("getMetadata error:", e.message);
      return [];
    }
  }

  async deleteVectors(namespace, vectorIds = []) {
    try {
      const { client } = await this.connect();

      await client.delete({
        collection_name: namespace,
        filter: `id in [${vectorIds.map((id) => `"${id}"`).join(",")}]`,
      });

      return { success: true };
    } catch (e) {
      console.error("deleteVectors error:", e.message);
      return { success: false, error: e.message };
    }
  }
}

module.exports.Milvus = Milvus;
