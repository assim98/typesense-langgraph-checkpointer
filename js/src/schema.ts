// js/src/schema.ts
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
