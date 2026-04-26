import os
import uuid

import pytest
import typesense
from langgraph_checkpoint_typesense.saver import (
    _checkpoint_doc_id,
    _write_doc_id,
    _ts_to_ms,
    _fv,
    AsyncTypesenseSaver,
    CHECKPOINTS_COLLECTION,
    WRITES_COLLECTION,
    CHECKPOINTS_SCHEMA,
    WRITES_SCHEMA,
)


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
    assert _fv("hello`world") == "helloworld"


def test_schemas_have_required_fields():
    cp_names = {f["name"] for f in CHECKPOINTS_SCHEMA["fields"]}
    assert {"thread_id", "checkpoint_id", "checkpoint_ns", "type", "checkpoint", "ts_ms"} <= cp_names
    wr_names = {f["name"] for f in WRITES_SCHEMA["fields"]}
    assert {"thread_id", "checkpoint_id", "task_id", "idx", "channel", "type", "value"} <= wr_names


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
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    thread_id = str(uuid.uuid4())
    cp = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    returned = await saver.aput(config, cp, {"source": "input", "step": -1, "parents": {}}, {})
    assert returned["configurable"]["thread_id"] == thread_id
    assert returned["configurable"]["checkpoint_id"] == cp["id"]


@pytest.mark.asyncio
async def test_aget_tuple_returns_latest_checkpoint(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    import time
    thread_id = str(uuid.uuid4())
    cp1 = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    time.sleep(0.05)  # ensure different ts_ms
    cp2 = create_checkpoint(cp1, {}, 1)
    await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})
    time.sleep(0.1)  # allow Typesense to index the document

    result = await saver.aget_tuple({"configurable": {"thread_id": thread_id}})
    assert result is not None
    assert result.checkpoint["id"] == cp2["id"]


@pytest.mark.asyncio
async def test_aget_tuple_by_checkpoint_id(saver):
    from langgraph.checkpoint.base import empty_checkpoint
    thread_id = str(uuid.uuid4())
    cp = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    saved_config = await saver.aput(config, cp, {"source": "input", "step": -1, "parents": {}}, {})

    result = await saver.aget_tuple(saved_config)
    assert result is not None
    assert result.checkpoint["id"] == cp["id"]


@pytest.mark.asyncio
async def test_aput_stores_parent_id(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    import time
    thread_id = str(uuid.uuid4())
    cp1 = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    time.sleep(0.05)
    cp2 = create_checkpoint(cp1, {}, 1)
    config2 = await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})

    result = await saver.aget_tuple(config2)
    assert result.parent_config["configurable"]["checkpoint_id"] == cp1["id"]


# ---- aput_writes tests ----

@pytest.mark.asyncio
async def test_aput_writes_stored_and_fetched(saver):
    from langgraph.checkpoint.base import empty_checkpoint
    import time
    thread_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())
    cp = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    saved_config = await saver.aput(config, cp, {"source": "input", "step": -1, "parents": {}}, {})

    await saver.aput_writes(saved_config, [("channel_a", "value1"), ("channel_b", 42)], task_id)
    time.sleep(0.1)

    result = await saver.aget_tuple(saved_config)
    assert result is not None
    assert len(result.pending_writes) == 2
    channels = {w[1] for w in result.pending_writes}
    assert channels == {"channel_a", "channel_b"}


# ---- alist tests ----

@pytest.mark.asyncio
async def test_alist_returns_all_checkpoints_latest_first(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    import time
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    cp1 = empty_checkpoint()
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    time.sleep(0.05)
    cp2 = create_checkpoint(cp1, {}, 1)
    await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})
    time.sleep(0.1)

    results = [t async for t in saver.alist({"configurable": {"thread_id": thread_id}})]
    assert len(results) == 2
    assert results[0].checkpoint["id"] == cp2["id"]
    assert results[1].checkpoint["id"] == cp1["id"]


@pytest.mark.asyncio
async def test_alist_limit(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    import time
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    cp1 = empty_checkpoint()
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    time.sleep(0.05)
    cp2 = create_checkpoint(cp1, {}, 1)
    await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})
    time.sleep(0.1)

    results = [t async for t in saver.alist({"configurable": {"thread_id": thread_id}}, limit=1)]
    assert len(results) == 1


@pytest.mark.asyncio
async def test_alist_filter_by_metadata(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    import time
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    cp1 = empty_checkpoint()
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    time.sleep(0.05)
    cp2 = create_checkpoint(cp1, {}, 1)
    await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})
    time.sleep(0.1)

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
    import time
    thread_id = str(uuid.uuid4())
    cp = empty_checkpoint()
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    await saver.aput(config, cp, {"source": "input", "step": -1, "parents": {}}, {})
    time.sleep(0.1)

    await saver.adelete_thread(thread_id)
    time.sleep(0.1)

    result = await saver.aget_tuple({"configurable": {"thread_id": thread_id}})
    assert result is None


@pytest.mark.asyncio
async def test_alist_before_parameter(saver):
    from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
    import time
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}

    cp1 = empty_checkpoint()
    config1 = await saver.aput(config, cp1, {"source": "input", "step": -1, "parents": {}}, {})
    time.sleep(0.05)
    cp2 = create_checkpoint(cp1, {}, 1)
    config2 = await saver.aput(config1, cp2, {"source": "loop", "step": 0, "parents": {}}, {})
    time.sleep(0.05)
    cp3 = create_checkpoint(cp2, {}, 2)
    await saver.aput(config2, cp3, {"source": "loop", "step": 1, "parents": {}}, {})
    time.sleep(0.1)

    # before=config2 means only checkpoints OLDER than cp2 (i.e. only cp1)
    results = [
        t async for t in saver.alist(
            {"configurable": {"thread_id": thread_id}}, before=config2
        )
    ]
    assert len(results) == 1
    assert results[0].checkpoint["id"] == cp1["id"]
