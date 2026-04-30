import { describe, it, expect, beforeAll, afterEach } from "vitest";
import { randomUUID } from "crypto";
import type { Checkpoint, CheckpointMetadata, CheckpointTuple } from "@langchain/langgraph-checkpoint";
import {
  checkpointDocId,
  writeDocId,
  tsToMs,
  filterVal,
  CHECKPOINTS_COLLECTION,
  WRITES_COLLECTION,
  CHECKPOINTS_SCHEMA,
  WRITES_SCHEMA,
  collectionDocuments,
} from "../src/schema";

describe("helpers", () => {
  it("checkpointDocId strips dashes", () => {
    expect(checkpointDocId("550e8400-e29b-41d4-a716-446655440000"))
      .toBe("550e8400e29b41d4a716446655440000");
  });

  it("writeDocId formats correctly", () => {
    const cid = "550e8400-e29b-41d4-a716-446655440000";
    const tid = "aaaabbbb-cccc-dddd-eeee-ffffffffffff";
    expect(writeDocId(cid, tid, 3))
      .toBe("550e8400e29b41d4a716446655440000_aaaabbbbccccddddeeeeffffffffffff_3");
  });

  it("tsToMs converts ISO 8601 to unix ms", () => {
    expect(tsToMs("2024-01-15T10:30:00.000Z")).toBe(1705314600000);
  });

  it("filterVal strips backticks", () => {
    expect(filterVal("hello`world")).toBe("helloworld");
  });

  it("schemas have required fields", () => {
    const cpNames = new Set(CHECKPOINTS_SCHEMA.fields.map((f) => f.name));
    expect(cpNames.has("thread_id")).toBe(true);
    expect(cpNames.has("checkpoint_id")).toBe(true);
    expect(cpNames.has("ts_ms")).toBe(true);
  });
});

import Typesense from "typesense";
import { TypesenseSaver } from "../src/TypesenseSaver";

const API_KEY = process.env.TYPESENSE_API_KEY ?? "test-api-key";
const client = new Typesense.Client({
  nodes: [{ host: "localhost", port: 8108, protocol: "http" }],
  apiKey: API_KEY,
  connectionTimeoutSeconds: 5,
});

// ---------------------------------------------------------------------------
// Polling helpers
// ---------------------------------------------------------------------------

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function pollUntil<T>(
  fn: () => Promise<T | null | undefined>,
  { timeout = 3000, interval = 50 } = {}
): Promise<T> {
  const deadline = Date.now() + timeout;
  while (true) {
    const result = await fn();
    if (result != null) return result as T;
    if (Date.now() >= deadline) throw new Error(`pollUntil timed out after ${timeout}ms`);
    await sleep(interval);
  }
}

async function pollListCount(
  gen: () => AsyncGenerator<CheckpointTuple>,
  count: number,
  { timeout = 3000, interval = 50 } = {}
): Promise<CheckpointTuple[]> {
  const deadline = Date.now() + timeout;
  while (true) {
    const results = await collectList(gen());
    if (results.length >= count) return results;
    if (Date.now() >= deadline) throw new Error(`Expected >= ${count} checkpoints, timed out`);
    await sleep(interval);
  }
}

async function collectList(gen: AsyncGenerator<CheckpointTuple>): Promise<CheckpointTuple[]> {
  const out: CheckpointTuple[] = [];
  for await (const t of gen) out.push(t);
  return out;
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

function makeCheckpoint(tsMs: number = Date.now()): Checkpoint {
  return {
    v: 1,
    id: randomUUID(),
    ts: new Date(tsMs).toISOString(),
    channel_values: {},
    channel_versions: {},
    versions_seen: {},
    pending_sends: [],
  };
}
const META: CheckpointMetadata = { source: "input", step: -1, parents: {} };

async function cleanCollections(): Promise<void> {
  for (const col of [CHECKPOINTS_COLLECTION, WRITES_COLLECTION]) {
    try {
      await collectionDocuments(client, col).delete({ filter_by: 'thread_id:!=""' });
    } catch { /* ignore */ }
  }
  // Brief wait for Typesense to process the deletes before the next test.
  await sleep(50);
}

// ---------------------------------------------------------------------------
// setup
// ---------------------------------------------------------------------------

describe("TypesenseSaver.setup", () => {
  beforeAll(async () => {
    for (const col of [CHECKPOINTS_COLLECTION, WRITES_COLLECTION]) {
      try { await client.collections(col).delete(); } catch { /* ignore */ }
    }
  });

  it("creates collections", async () => {
    const saver = new TypesenseSaver(client);
    await saver.setup();
    const cols = await client.collections().retrieve();
    const names = (cols as Array<{ name: string }>).map((c) => c.name);
    expect(names).toContain(CHECKPOINTS_COLLECTION);
    expect(names).toContain(WRITES_COLLECTION);
  });

  it("is idempotent", async () => {
    const saver = new TypesenseSaver(client);
    await expect(saver.setup()).resolves.not.toThrow();
  });

  it("fromConfig creates a saver", () => {
    const s = TypesenseSaver.fromConfig({ apiKey: API_KEY });
    expect(s).toBeInstanceOf(TypesenseSaver);
  });
});

// ---------------------------------------------------------------------------
// put + getTuple
// ---------------------------------------------------------------------------

describe("put + getTuple", () => {
  let saver: TypesenseSaver;
  beforeAll(async () => { saver = new TypesenseSaver(client); await saver.setup(); });
  afterEach(cleanCollections);

  it("put returns config with checkpoint_id", async () => {
    const threadId = randomUUID();
    const cp = makeCheckpoint();
    const ret = await saver.put({ configurable: { thread_id: threadId, checkpoint_ns: "" } }, cp, META, {});
    expect(ret.configurable?.checkpoint_id).toBe(cp.id);
    expect(ret.configurable?.thread_id).toBe(threadId);
  });

  it("getTuple returns undefined for missing thread", async () => {
    expect(await saver.getTuple({ configurable: { thread_id: randomUUID() } })).toBeUndefined();
  });

  it("getTuple returns latest checkpoint", async () => {
    const threadId = randomUUID();
    const cp1 = makeCheckpoint(1000);
    const c1 = await saver.put({ configurable: { thread_id: threadId, checkpoint_ns: "" } }, cp1, META, {});
    const cp2 = makeCheckpoint(2000);
    await saver.put(c1, cp2, { source: "loop", step: 0, parents: {} }, {});
    const result = await pollUntil(() => saver.getTuple({ configurable: { thread_id: threadId } }));
    expect(result.checkpoint.id).toBe(cp2.id);
  });

  it("getTuple returns checkpoint by id", async () => {
    const threadId = randomUUID();
    const cp = makeCheckpoint();
    const saved = await saver.put({ configurable: { thread_id: threadId, checkpoint_ns: "" } }, cp, META, {});
    const result = await pollUntil(() => saver.getTuple(saved));
    expect(result.checkpoint.id).toBe(cp.id);
  });

  it("put stores parent_id", async () => {
    const threadId = randomUUID();
    const cp1 = makeCheckpoint(1000);
    const c1 = await saver.put({ configurable: { thread_id: threadId, checkpoint_ns: "" } }, cp1, META, {});
    const cp2 = makeCheckpoint(2000);
    const c2 = await saver.put(c1, cp2, { source: "loop", step: 0, parents: {} }, {});
    const result = await pollUntil(() => saver.getTuple(c2));
    expect(result.parentConfig?.configurable?.checkpoint_id).toBe(cp1.id);
  });
});

// ---------------------------------------------------------------------------
// putWrites
// ---------------------------------------------------------------------------

describe("putWrites", () => {
  let saver: TypesenseSaver;
  beforeAll(async () => { saver = new TypesenseSaver(client); await saver.setup(); });
  afterEach(cleanCollections);

  it("stores writes retrievable via getTuple.pendingWrites", async () => {
    const threadId = randomUUID();
    const taskId = randomUUID();
    const cp = makeCheckpoint();
    const saved = await saver.put({ configurable: { thread_id: threadId, checkpoint_ns: "" } }, cp, META, {});
    await saver.putWrites(saved, [["channel_a", "hello"], ["channel_b", 42]], taskId);
    const result = await pollUntil(async () => {
      const t = await saver.getTuple(saved);
      return t?.pendingWrites?.length === 2 ? t : null;
    });
    const channels = result.pendingWrites?.map((w) => w[1]);
    expect(channels).toContain("channel_a");
    expect(channels).toContain("channel_b");
  });

  it("stores task_path when provided", async () => {
    const threadId = randomUUID();
    const cp = makeCheckpoint();
    const saved = await saver.put({ configurable: { thread_id: threadId, checkpoint_ns: "" } }, cp, META, {});
    await saver.putWrites(saved, [["ch", "v"]], randomUUID(), "root:0");
    // No assertion on the stored value (internal field), just ensure no error thrown.
  });
});

// ---------------------------------------------------------------------------
// deleteThread
// ---------------------------------------------------------------------------

describe("deleteThread", () => {
  let saver: TypesenseSaver;
  beforeAll(async () => { saver = new TypesenseSaver(client); await saver.setup(); });

  it("removes all checkpoints", async () => {
    const threadId = randomUUID();
    const saved = await saver.put(
      { configurable: { thread_id: threadId, checkpoint_ns: "" } },
      makeCheckpoint(), META, {}
    );
    await pollUntil(() => saver.getTuple(saved));
    await saver.deleteThread(threadId);
    await pollUntil(async () => {
      const r = await saver.getTuple({ configurable: { thread_id: threadId } });
      return r === undefined ? true : null;
    });
    expect(await saver.getTuple({ configurable: { thread_id: threadId } })).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

describe("list", () => {
  let saver: TypesenseSaver;
  beforeAll(async () => { saver = new TypesenseSaver(client); await saver.setup(); });
  afterEach(cleanCollections);

  it("returns all checkpoints latest-first", async () => {
    const threadId = randomUUID();
    const cp1 = makeCheckpoint(1000);
    const c1 = await saver.put({ configurable: { thread_id: threadId, checkpoint_ns: "" } }, cp1, META, {});
    const cp2 = makeCheckpoint(2000);
    await saver.put(c1, cp2, { source: "loop", step: 0, parents: {} }, {});
    const results = await pollListCount(
      () => saver.list({ configurable: { thread_id: threadId } }), 2
    );
    expect(results[0].checkpoint.id).toBe(cp2.id);
  });

  it("respects limit", async () => {
    const threadId = randomUUID();
    const cp1 = makeCheckpoint(1000);
    const c1 = await saver.put({ configurable: { thread_id: threadId, checkpoint_ns: "" } }, cp1, META, {});
    await saver.put(c1, makeCheckpoint(2000), { source: "loop", step: 0, parents: {} }, {});
    await pollListCount(() => saver.list({ configurable: { thread_id: threadId } }), 2);
    const results = await collectList(saver.list({ configurable: { thread_id: threadId } }, { limit: 1 }));
    expect(results).toHaveLength(1);
  });

  it("filters by metadata", async () => {
    const threadId = randomUUID();
    const cp1 = makeCheckpoint(1000);
    const c1 = await saver.put({ configurable: { thread_id: threadId, checkpoint_ns: "" } }, cp1, META, {});
    const cp2 = makeCheckpoint(2000);
    await saver.put(c1, cp2, { source: "loop", step: 0, parents: {} }, {});
    await pollListCount(() => saver.list({ configurable: { thread_id: threadId } }), 2);
    const results = await collectList(
      saver.list({ configurable: { thread_id: threadId } }, { filter: { source: "loop" } })
    );
    expect(results).toHaveLength(1);
    expect(results[0].checkpoint.id).toBe(cp2.id);
  });
});
