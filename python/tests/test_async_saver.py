import asyncio
import os
import uuid

import pytest
import typesense
from langgraph_checkpoint_typesense.saver import (
    _checkpoint_doc_id,
    _write_doc_id,
    _ts_to_ms,
    _sanitize_filter_value,
    AsyncTypesenseSaver,
    CHECKPOINTS_COLLECTION,
    WRITES_COLLECTION,
    CHECKPOINTS_SCHEMA,
    WRITES_SCHEMA,
)


# ---------------------------------------------------------------------------
# Polling helpers
# ---------------------------------------------------------------------------

async def _poll_until(fn, *, timeout: float = 3.0, interval: float = 0.05):
    """Poll async callable fn() until it returns a truthy value."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        result = await fn()
        if result:
            return result
        if loop.time() >= deadline:
            raise TimeoutError("Condition not met within timeout")
        await asyncio.sleep(interval)


async def _poll_list(saver, config, n, *, timeout: float = 3.0, **kwargs):
    """Poll alist until at least n checkpoints are indexed."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        results = [t async for t in saver.alist(config, **kwargs)]
        if len(results) >= n:
            return results
        if loop.time() >= deadline:
            raise TimeoutError(f"Expected >= {n} checkpoints, got {len(results)}")
        await asyncio.sleep(0.05)


async def _poll_deleted(saver, config, *, timeout: float = 3.0):
    """Poll aget_tuple until the document is gone (returns None)."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        if await saver.aget_tuple(config) is None:
            return
        if loop.time() >= deadline:
            raise TimeoutError("Document not deleted within timeout")
        await asyncio.sleep(0.05)


# ---------------------------------------------------------------------------
# Unit tests (no Typesense needed)
# ---------------------------------------------------------------------------

def test_checkpoint_doc_id_strips_dashes():
    assert _checkpoint_doc_id("550e8400-e29b-41d4-a716-446655440000") == "550e8400e29b41d4a716446655440000"


def test_write_doc_id_format():
    cid = "550e8400-e29b-41d4-a716-446655440000"
    tid = "aaaabbbb-cccc-dddd-eeee-ffffffffffff"
    result = _write_doc_id(cid, tid, 3)
    assert result == "550e8400e29b41d4a716446655440000_aaaabbbbccccddddeeeeffffffffffff_3"


def test_ts_to_ms_iso():
    ms = _ts_to_ms("2024-01-15T10:30:00.000Z")
    assert ms == 1705314600000


def test_fv_escapes_backtick():
    assert _sanitize_filter_value("hello`world") == "helloworld"


def test_schemas_have_required_fields():
    cp_names = {f["name"] for f in CHECKPOINTS_SCHEMA["fields"]}
    assert {"thread_id", "checkpoint_id", "checkpoint_ns", "type", "checkpoint", "ts_ms"} <= cp_names
    wr_names = {f["name"] for f in WRITES_SCHEMA["fields"]}
    assert {"thread_id", "checkpoint_id", "task_id", "idx", "channel", "type", "value"} <= wr_names


# ---------------------------------------------------------------------------
# Integration tests (require a running Typesense instance)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_setup_creates_collections(ts_client):
    saver = AsyncTypesenseSaver(ts_client)
    await saver.setup()
    cols = {c["name"] for c in ts_client.collections.retrieve()}
    assert CHECKPOINTS_COLLECTION in cols
    assert WRITES_COLLECTION in cols


@pytest.mark.asyncio
async def test_setup_idempotent(ts_client):
    saver = AsyncTypesenseSaver(ts_client)
    await saver.setup()
    await saver.setup()  # must not raise


@pytest.mark.asyncio
async def test_from_config_creates_client():
    api_key = os.environ.get("TYPESENSE_API_KEY", "test-api-key")
    saver = AsyncTypesenseSaver.from_config(
        host="localhost", port=8108, api_key=api_key
    )
    assert isinstance(saver, AsyncTypesenseSaver)


# ---- aget_tuple tests ----

@pytest.mark.asyncio
async def test_aget_tuple_returns_none_for_missing(saver):
    result = await saver.aget_tuple({"configurable": {"thread_id": str(uuid.uuid4())}})
    assert result is None


@pytest.mark.asyncio
async def test_aput_returns_config_with_checkpoint_id(saver):
    from langgraph.checkpoint.base import empty_checkpoint
    thread_id = str(uuid.uuid4())
    cp = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    returned = await saver.aput(config, cp, {"source": "input", "step": -1, "parents": {}}, {})
    assert returned["configurable"]["thread_id"] == thread_id
    assert returned["configurable"]["checkpoint_id"] == cp["id"]


@pytest.mark.asyncio
async def test_aget_tuple_returns_latest_checkpoint(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    thread_id = str(uuid.uuid4())
    cp1 = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    await asyncio.sleep(0.05)  # ensure distinct ts_ms values
    cp2 = create_checkpoint(cp1, {}, 1)
    await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})

    result = await _poll_until(
        lambda: saver.aget_tuple({"configurable": {"thread_id": thread_id}})
    )
    assert result.checkpoint["id"] == cp2["id"]


@pytest.mark.asyncio
async def test_aget_tuple_by_checkpoint_id(saver):
    from langgraph.checkpoint.base import empty_checkpoint
    thread_id = str(uuid.uuid4())
    cp = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    saved_config = await saver.aput(config, cp, {"source": "input", "step": -1, "parents": {}}, {})

    result = await _poll_until(lambda: saver.aget_tuple(saved_config))
    assert result.checkpoint["id"] == cp["id"]


@pytest.mark.asyncio
async def test_aput_stores_parent_id(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    thread_id = str(uuid.uuid4())
    cp1 = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    await asyncio.sleep(0.05)  # ensure distinct ts_ms values
    cp2 = create_checkpoint(cp1, {}, 1)
    config2 = await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})

    result = await _poll_until(lambda: saver.aget_tuple(config2))
    assert result.parent_config["configurable"]["checkpoint_id"] == cp1["id"]


# ---- aput_writes tests ----

@pytest.mark.asyncio
async def test_aput_writes_stored_and_fetched(saver):
    from langgraph.checkpoint.base import empty_checkpoint
    thread_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())
    cp = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    saved_config = await saver.aput(config, cp, {"source": "input", "step": -1, "parents": {}}, {})

    await saver.aput_writes(saved_config, [("channel_a", "value1"), ("channel_b", 42)], task_id)

    async def _has_writes():
        r = await saver.aget_tuple(saved_config)
        return r if (r and r.pending_writes and len(r.pending_writes) == 2) else None

    result = await _poll_until(_has_writes)
    channels = {w[1] for w in result.pending_writes}
    assert channels == {"channel_a", "channel_b"}


# ---- alist tests ----

@pytest.mark.asyncio
async def test_alist_returns_all_checkpoints_latest_first(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    cp1 = empty_checkpoint()
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    await asyncio.sleep(0.05)  # ensure distinct ts_ms values
    cp2 = create_checkpoint(cp1, {}, 1)
    await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})

    results = await _poll_list(saver, {"configurable": {"thread_id": thread_id}}, 2)
    assert results[0].checkpoint["id"] == cp2["id"]
    assert results[1].checkpoint["id"] == cp1["id"]


@pytest.mark.asyncio
async def test_alist_limit(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    cp1 = empty_checkpoint()
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    await asyncio.sleep(0.05)
    cp2 = create_checkpoint(cp1, {}, 1)
    await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})

    await _poll_list(saver, {"configurable": {"thread_id": thread_id}}, 2)
    results = [t async for t in saver.alist({"configurable": {"thread_id": thread_id}}, limit=1)]
    assert len(results) == 1


@pytest.mark.asyncio
async def test_alist_filter_by_metadata(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    cp1 = empty_checkpoint()
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    await asyncio.sleep(0.05)
    cp2 = create_checkpoint(cp1, {}, 1)
    await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})

    await _poll_list(saver, {"configurable": {"thread_id": thread_id}}, 2)
    results = [
        t async for t in saver.alist(
            {"configurable": {"thread_id": thread_id}}, filter={"source": "loop"}
        )
    ]
    assert len(results) == 1
    assert results[0].checkpoint["id"] == cp2["id"]


# ---- adelete_thread tests ----

@pytest.mark.asyncio
async def test_adelete_thread_removes_all(saver):
    from langgraph.checkpoint.base import empty_checkpoint
    thread_id = str(uuid.uuid4())
    cp = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    await saver.aput(config, cp, {"source": "input", "step": -1, "parents": {}}, {})

    await _poll_until(lambda: saver.aget_tuple({"configurable": {"thread_id": thread_id}}))
    await saver.adelete_thread(thread_id)
    await _poll_deleted(saver, {"configurable": {"thread_id": thread_id}})

    result = await saver.aget_tuple({"configurable": {"thread_id": thread_id}})
    assert result is None


@pytest.mark.asyncio
async def test_alist_before_parameter(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}

    cp1 = empty_checkpoint()
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    await asyncio.sleep(0.05)
    cp2 = create_checkpoint(cp1, {}, 1)
    config2 = await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})
    await asyncio.sleep(0.05)
    cp3 = create_checkpoint(cp2, {}, 2)
    await saver.aput(config2, cp3, {"source": "loop", "step": 1, "parents": {}}, {})

    await _poll_list(saver, {"configurable": {"thread_id": thread_id}}, 3)

    # before=config2 means only checkpoints OLDER than cp2 (i.e. only cp1)
    results = [
        t async for t in saver.alist(
            {"configurable": {"thread_id": thread_id}}, before=config2
        )
    ]
    assert len(results) == 1
    assert results[0].checkpoint["id"] == cp1["id"]
