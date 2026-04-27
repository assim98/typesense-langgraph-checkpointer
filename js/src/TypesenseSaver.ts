// js/src/TypesenseSaver.ts
import { Client as TypesenseClient } from "typesense";
import {
  BaseCheckpointSaver,
  type Checkpoint,
  type CheckpointMetadata,
  type CheckpointTuple,
  type CheckpointListOptions,
  type ChannelVersions,
  type SerializerProtocol,
  type PendingWrite,
} from "@langchain/langgraph-checkpoint";
import type { RunnableConfig } from "@langchain/core/runnables";
import {
  CHECKPOINTS_COLLECTION,
  WRITES_COLLECTION,
  CHECKPOINTS_SCHEMA,
  WRITES_SCHEMA,
  checkpointDocId,
  writeDocId,
  tsToMs,
  filterVal,
} from "./schema";

export class TypesenseSaver extends BaseCheckpointSaver {
  private client: TypesenseClient;

  constructor(client: TypesenseClient, serde?: SerializerProtocol) {
    super(serde);
    this.client = client;
  }

  static fromConfig(config: {
    host?: string;
    port?: number;
    apiKey: string;
    protocol?: string;
    connectionTimeoutSeconds?: number;
    serde?: SerializerProtocol;
  }): TypesenseSaver {
    const client = new TypesenseClient({
      nodes: [{
        host: config.host ?? "localhost",
        port: config.port ?? 8108,
        protocol: config.protocol ?? "http",
      }],
      apiKey: config.apiKey,
      connectionTimeoutSeconds: config.connectionTimeoutSeconds ?? 5,
    });
    return new TypesenseSaver(client, config.serde);
  }

  async setup(): Promise<void> {
    for (const schema of [CHECKPOINTS_SCHEMA, WRITES_SCHEMA]) {
      try {
        await this.client.collections(schema.name).retrieve();
      } catch (e: unknown) {
        if ((e as { httpStatus?: number }).httpStatus === 404) {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          await this.client.collections().create(schema as any);
        } else {
          throw e;
        }
      }
    }
  }

  private async docToTuple(
    doc: Record<string, unknown>,
    pendingWrites: Array<[string, string, unknown]>
  ): Promise<CheckpointTuple> {
    const checkpoint = await this.serde.loadsTyped(
      doc.type as string,
      Buffer.from(doc.checkpoint as string, "base64")
    ) as Checkpoint;
    const metadata = await this.serde.loadsTyped(
      doc.metadata_type as string,
      Buffer.from(doc.metadata as string, "base64")
    ) as CheckpointMetadata;

    const threadId = doc.thread_id as string;
    const checkpointNs = doc.checkpoint_ns as string;
    const checkpointId = doc.checkpoint_id as string;
    const parentId = (doc.parent_checkpoint_id as string | undefined) || undefined;

    const config: RunnableConfig = {
      configurable: { thread_id: threadId, checkpoint_ns: checkpointNs, checkpoint_id: checkpointId },
    };
    const parentConfig: RunnableConfig | undefined = parentId
      ? { configurable: { thread_id: threadId, checkpoint_ns: checkpointNs, checkpoint_id: parentId } }
      : undefined;

    return { config, checkpoint, metadata, parentConfig, pendingWrites };
  }

  private async fetchPendingWrites(
    threadId: string,
    checkpointId: string
  ): Promise<Array<[string, string, unknown]>> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const results: any = await (this.client.collections(WRITES_COLLECTION).documents() as any).search({
      q: "*",
      query_by: "checkpoint_id",
      filter_by: `checkpoint_id:=\`${filterVal(checkpointId)}\` && thread_id:=\`${filterVal(threadId)}\``,
      per_page: 250,
      sort_by: "idx:asc",
    });
    return Promise.all(
      (results.hits ?? []).map(async (h: { document: Record<string, unknown> }) => [
        h.document.task_id as string,
        h.document.channel as string,
        await this.serde.loadsTyped(
          h.document.type as string,
          Buffer.from(h.document.value as string, "base64")
        ),
      ])
    );
  }

  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    const cfg = (config.configurable ?? {}) as Record<string, string>;
    const threadId = cfg.thread_id;
    const checkpointNs = cfg.checkpoint_ns ?? "";
    const checkpointId = cfg.checkpoint_id;

    if (!threadId && !checkpointId) return undefined;

    let doc: Record<string, unknown>;

    if (checkpointId) {
      try {
        doc = await this.client
          .collections(CHECKPOINTS_COLLECTION)
          .documents(checkpointDocId(checkpointId))
          .retrieve() as Record<string, unknown>;
      } catch (e: unknown) {
        if ((e as { httpStatus?: number }).httpStatus === 404) return undefined;
        throw e;
      }
    } else {
      const filterParts = [`thread_id:=\`${filterVal(threadId)}\``];
      if (checkpointNs) filterParts.push(`checkpoint_ns:=\`${filterVal(checkpointNs)}\``);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const results: any = await (this.client.collections(CHECKPOINTS_COLLECTION).documents() as any).search({
        q: "*",
        query_by: "thread_id",
        filter_by: filterParts.join(" && "),
        per_page: 1,
        sort_by: "ts_ms:desc",
      });
      const hits = results.hits ?? [];
      if (!hits.length) return undefined;
      doc = hits[0].document;
    }

    const pending = await this.fetchPendingWrites(doc.thread_id as string, doc.checkpoint_id as string);
    return await this.docToTuple(doc, pending);
  }

  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    _newVersions: ChannelVersions
  ): Promise<RunnableConfig> {
    const cfg = (config.configurable ?? {}) as Record<string, string>;
    const threadId = cfg.thread_id;
    const checkpointNs = cfg.checkpoint_ns ?? "";
    const checkpointId = checkpoint.id;
    const parentId = cfg.checkpoint_id || undefined;

    const [cpType, cpBytes] = await this.serde.dumpsTyped(checkpoint);
    const [metaType, metaBytes] = await this.serde.dumpsTyped(metadata);

    const doc: Record<string, unknown> = {
      id: checkpointDocId(checkpointId),
      thread_id: threadId,
      checkpoint_ns: checkpointNs,
      checkpoint_id: checkpointId,
      type: cpType,
      checkpoint: Buffer.from(cpBytes).toString("base64"),
      metadata_type: metaType,
      metadata: Buffer.from(metaBytes).toString("base64"),
      ts_ms: tsToMs(checkpoint.ts),
    };
    if (parentId) doc.parent_checkpoint_id = parentId;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (this.client.collections(CHECKPOINTS_COLLECTION).documents() as any).upsert(doc);

    return { configurable: { thread_id: threadId, checkpoint_ns: checkpointNs, checkpoint_id: checkpointId } };
  }

  async putWrites(
    config: RunnableConfig,
    writes: PendingWrite[],
    taskId: string
  ): Promise<void> {
    const cfg = (config.configurable ?? {}) as Record<string, string>;
    const { thread_id: threadId, checkpoint_ns: checkpointNs = "", checkpoint_id: checkpointId } = cfg;
    if (!checkpointId) throw new Error("config must contain checkpoint_id for putWrites");

    for (let idx = 0; idx < writes.length; idx++) {
      const [channel, value] = writes[idx];
      const [valType, valBytes] = await this.serde.dumpsTyped(value);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await (this.client.collections(WRITES_COLLECTION).documents() as any).upsert({
        id: writeDocId(checkpointId, taskId, idx),
        thread_id: threadId,
        checkpoint_ns: checkpointNs,
        checkpoint_id: checkpointId,
        task_id: taskId,
        idx,
        channel,
        type: valType,
        value: Buffer.from(valBytes).toString("base64"),
      });
    }
  }

  async deleteThread(threadId: string): Promise<void> {
    for (const col of [CHECKPOINTS_COLLECTION, WRITES_COLLECTION]) {
      await (this.client.collections(col).documents() as any).delete({
        filter_by: `thread_id:=\`${filterVal(threadId)}\``,
      });
    }
  }

  async *list(
    config: RunnableConfig | null | undefined,
    options?: CheckpointListOptions
  ): AsyncGenerator<CheckpointTuple> {
    const cfg = ((config?.configurable ?? {}) as Record<string, string>);
    const threadId = cfg.thread_id;
    const checkpointNs = cfg.checkpoint_ns ?? "";
    const { limit, before, filter } = options ?? {};

    const filterParts: string[] = [];
    if (threadId) {
      filterParts.push(`thread_id:=\`${filterVal(threadId)}\``);
      if (checkpointNs) filterParts.push(`checkpoint_ns:=\`${filterVal(checkpointNs)}\``);
    }

    if (before?.configurable?.checkpoint_id) {
      try {
        const beforeDoc = await this.client
          .collections(CHECKPOINTS_COLLECTION)
          .documents(checkpointDocId(before.configurable.checkpoint_id as string))
          .retrieve() as Record<string, unknown>;
        filterParts.push(`ts_ms:<${beforeDoc.ts_ms}`);
      } catch { return; }
    }

    if (limit !== undefined && limit <= 0) return;

    const filterBy = filterParts.length ? filterParts.join(" && ") : undefined;
    let page = 1;
    const perPage = 250;
    let remaining = limit;

    while (true) {
      const thisPage = remaining !== undefined ? Math.min(perPage, remaining) : perPage;
      const params: Record<string, unknown> = { q: "*", query_by: "thread_id", per_page: thisPage, sort_by: "ts_ms:desc", page };
      if (filterBy) params.filter_by = filterBy;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const results: any = await (this.client.collections(CHECKPOINTS_COLLECTION).documents() as any).search(params);
      const hits: Array<{ document: Record<string, unknown> }> = results.hits ?? [];
      if (!hits.length) break;

      for (const hit of hits) {
        const doc = hit.document;
        if (filter) {
          const meta = await this.serde.loadsTyped(
            doc.metadata_type as string,
            Buffer.from(doc.metadata as string, "base64")
          ) as Record<string, unknown>;
          if (!Object.entries(filter).every(([k, v]) => meta[k] === v)) continue;
        }
        yield await this.docToTuple(doc, []);
        if (remaining !== undefined && --remaining <= 0) return;
      }

      if (hits.length < perPage) break;
      page++;
    }
  }
}
