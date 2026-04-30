// js/src/schema.ts
import type { Client as TypesenseClient } from "typesense";

/** Minimal typed surface of the Typesense Documents API used by this library. */
interface TypesenseDocumentsApi {
  search(params: Record<string, unknown>): Promise<{
    hits?: Array<{ document: Record<string, unknown> }>;
  }>;
  upsert(doc: Record<string, unknown>): Promise<Record<string, unknown>>;
  delete(params: { filter_by: string }): Promise<unknown>;
}

/**
 * Return a typed handle to a collection's documents endpoint.
 * The single `as any` cast is isolated here so the rest of the codebase
 * remains type-safe.
 */
export function collectionDocuments(
  client: TypesenseClient,
  collection: string
): TypesenseDocumentsApi {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return client.collections(collection).documents() as any;
}

export const CHECKPOINTS_COLLECTION = "langgraph_checkpoints";
export const WRITES_COLLECTION = "langgraph_checkpoint_writes";

export const CHECKPOINTS_SCHEMA = {
  name: CHECKPOINTS_COLLECTION,
  fields: [
    { name: "thread_id", type: "string" },
    { name: "checkpoint_ns", type: "string" },
    { name: "checkpoint_id", type: "string" },
    { name: "parent_checkpoint_id", type: "string", optional: true },
    { name: "type", type: "string" },
    { name: "checkpoint", type: "string" },
    { name: "metadata_type", type: "string" },
    { name: "metadata", type: "string" },
    { name: "ts_ms", type: "int64" },
  ],
  default_sorting_field: "ts_ms",
};

export const WRITES_SCHEMA = {
  name: WRITES_COLLECTION,
  fields: [
    { name: "thread_id", type: "string" },
    { name: "checkpoint_ns", type: "string" },
    { name: "checkpoint_id", type: "string" },
    { name: "task_id", type: "string" },
    { name: "idx", type: "int32" },
    { name: "channel", type: "string" },
    { name: "type", type: "string" },
    { name: "value", type: "string" },
    { name: "task_path", type: "string", optional: true },
  ],
};

export function checkpointDocId(checkpointId: string): string {
  return checkpointId.replaceAll("-", "");
}

export function writeDocId(checkpointId: string, taskId: string, idx: number): string {
  return `${checkpointId.replaceAll("-", "")}_${taskId.replaceAll("-", "")}_${idx}`;
}

export function tsToMs(ts: string): number {
  return new Date(ts).getTime();
}

export function filterVal(val: string): string {
  return val.replaceAll("`", "");
}
