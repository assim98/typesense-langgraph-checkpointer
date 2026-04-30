from __future__ import annotations

import asyncio
import base64
from datetime import datetime
from typing import Any, AsyncIterator, Iterator, Optional, Sequence, cast

import typesense
import typesense.exceptions
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
)

CHECKPOINTS_COLLECTION = "langgraph_checkpoints"
WRITES_COLLECTION = "langgraph_checkpoint_writes"

CHECKPOINTS_SCHEMA: dict[str, Any] = {
    "name": CHECKPOINTS_COLLECTION,
    "fields": [
        {"name": "thread_id", "type": "string"},
        {"name": "checkpoint_ns", "type": "string"},
        {"name": "checkpoint_id", "type": "string"},
        {"name": "parent_checkpoint_id", "type": "string", "optional": True},
        {"name": "type", "type": "string"},
        {"name": "checkpoint", "type": "string"},
        {"name": "metadata_type", "type": "string"},
        {"name": "metadata", "type": "string"},
        {"name": "ts_ms", "type": "int64"},
    ],
    "default_sorting_field": "ts_ms",
}

WRITES_SCHEMA: dict[str, Any] = {
    "name": WRITES_COLLECTION,
    "fields": [
        {"name": "thread_id", "type": "string"},
        {"name": "checkpoint_ns", "type": "string"},
        {"name": "checkpoint_id", "type": "string"},
        {"name": "task_id", "type": "string"},
        {"name": "idx", "type": "int32"},
        {"name": "channel", "type": "string"},
        {"name": "type", "type": "string"},
        {"name": "value", "type": "string"},
        {"name": "task_path", "type": "string", "optional": True},
    ],
}


def _checkpoint_doc_id(checkpoint_id: str) -> str:
    return checkpoint_id.replace("-", "")


def _write_doc_id(checkpoint_id: str, task_id: str, idx: int) -> str:
    return f"{checkpoint_id.replace('-', '')}_{task_id.replace('-', '')}_{idx}"


def _ts_to_ms(ts: str) -> int:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def _sanitize_filter_value(val: str) -> str:
    """Strip backticks from filter values to prevent Typesense filter injection."""
    return val.replace("`", "")


class AsyncTypesenseSaver(BaseCheckpointSaver):
    """Async LangGraph checkpoint saver backed by Typesense.

    Requires calling ``await saver.setup()`` once before use.
    """

    def __init__(self, client: typesense.Client) -> None:
        super().__init__()
        self._client = client

    @classmethod
    def from_config(
        cls,
        *,
        host: str = "localhost",
        port: int = 8108,
        api_key: str,
        protocol: str = "http",
        connection_timeout_seconds: int = 5,
    ) -> "AsyncTypesenseSaver":
        client = typesense.Client({
            "nodes": [{"host": host, "port": str(port), "protocol": protocol}],  # type: ignore[list-item]
            "api_key": api_key,
            "connection_timeout_seconds": connection_timeout_seconds,
        })
        return cls(client)

    async def setup(self) -> None:
        """Create Typesense collections if they do not exist."""
        for schema in [CHECKPOINTS_SCHEMA, WRITES_SCHEMA]:
            try:
                await asyncio.to_thread(
                    self._client.collections[schema["name"]].retrieve
                )
            except typesense.exceptions.ObjectNotFound:
                await asyncio.to_thread(
                    self._client.collections.create, schema  # type: ignore[arg-type]
                )

    def _doc_to_tuple(
        self, doc: dict[str, Any], pending_writes: list[tuple[str, str, Any]]
    ) -> CheckpointTuple:
        checkpoint: Checkpoint = self.serde.loads_typed(
            (doc["type"], base64.b64decode(doc["checkpoint"]))
        )
        metadata: CheckpointMetadata = self.serde.loads_typed(
            (doc["metadata_type"], base64.b64decode(doc["metadata"]))
        )
        thread_id: str = doc["thread_id"]
        checkpoint_ns: str = doc["checkpoint_ns"]
        checkpoint_id: str = doc["checkpoint_id"]
        parent_checkpoint_id: Optional[str] = doc.get("parent_checkpoint_id") or None

        config: RunnableConfig = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }
        parent_config: Optional[RunnableConfig] = (
            {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": parent_checkpoint_id,
                }
            }
            if parent_checkpoint_id
            else None
        )
        return CheckpointTuple(
            config=config,
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes or None,
        )

    async def _fetch_pending_writes(
        self, thread_id: str, checkpoint_id: str
    ) -> list[tuple[str, str, Any]]:
        results = await asyncio.to_thread(
            self._client.collections[WRITES_COLLECTION].documents.search,
            {  # type: ignore[arg-type]
                "q": "*",
                "query_by": "checkpoint_id",
                "filter_by": (
                    f"checkpoint_id:=`{_sanitize_filter_value(checkpoint_id)}` && "
                    f"thread_id:=`{_sanitize_filter_value(thread_id)}`"
                ),
                "per_page": 250,
                "sort_by": "idx:asc",
            },
        )
        return [
            (
                d["task_id"],
                d["channel"],
                self.serde.loads_typed((d["type"], base64.b64decode(d["value"]))),
            )
            for h in results.get("hits", [])
            for d in [cast(dict[str, Any], h["document"])]
        ]

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        configurable = config.get("configurable", {})
        thread_id: str = configurable["thread_id"]
        checkpoint_ns: str = configurable.get("checkpoint_ns", "")
        checkpoint_id: Optional[str] = get_checkpoint_id(config)

        if checkpoint_id:
            try:
                doc = await asyncio.to_thread(
                    self._client.collections[CHECKPOINTS_COLLECTION]
                    .documents[_checkpoint_doc_id(checkpoint_id)]
                    .retrieve
                )
            except typesense.exceptions.ObjectNotFound:
                return None
        else:
            # Typesense does not support filtering on empty strings with backtick
            # syntax, so only add checkpoint_ns to the filter when it is non-empty.
            filter_by = f"thread_id:=`{_sanitize_filter_value(thread_id)}`"
            if checkpoint_ns:
                filter_by += f" && checkpoint_ns:=`{_sanitize_filter_value(checkpoint_ns)}`"
            results = await asyncio.to_thread(
                self._client.collections[CHECKPOINTS_COLLECTION].documents.search,
                {
                    "q": "*",
                    "query_by": "thread_id",
                    "filter_by": filter_by,
                    "per_page": 1,
                    "sort_by": "ts_ms:desc",
                },
            )
            hits = results.get("hits", [])
            if not hits:
                return None
            doc = cast(dict[str, Any], hits[0]["document"])

        pending_writes = await self._fetch_pending_writes(thread_id, doc["checkpoint_id"])
        return self._doc_to_tuple(doc, pending_writes)

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: Any,
    ) -> RunnableConfig:
        configurable = config.get("configurable", {})
        thread_id: str = configurable["thread_id"]
        checkpoint_ns: str = configurable.get("checkpoint_ns", "")
        checkpoint_id: str = checkpoint["id"]
        parent_checkpoint_id: Optional[str] = get_checkpoint_id(config)

        cp_type, cp_bytes = self.serde.dumps_typed(checkpoint)
        meta_type, meta_bytes = self.serde.dumps_typed(dict(metadata))

        doc: dict[str, Any] = {
            "id": _checkpoint_doc_id(checkpoint_id),
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "type": cp_type,
            "checkpoint": base64.b64encode(cp_bytes).decode(),
            "metadata_type": meta_type,
            "metadata": base64.b64encode(meta_bytes).decode(),
            "ts_ms": _ts_to_ms(checkpoint["ts"]),
        }
        if parent_checkpoint_id:
            doc["parent_checkpoint_id"] = parent_checkpoint_id

        await asyncio.to_thread(
            self._client.collections[CHECKPOINTS_COLLECTION].documents.upsert, doc
        )

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        configurable = config.get("configurable", {})
        thread_id: str = configurable["thread_id"]
        checkpoint_ns: str = configurable.get("checkpoint_ns", "")
        checkpoint_id: Optional[str] = get_checkpoint_id(config)
        if checkpoint_id is None:
            raise ValueError("config must contain checkpoint_id for aput_writes")

        for idx, (channel, value) in enumerate(writes):
            val_type, val_bytes = self.serde.dumps_typed(value)
            doc: dict[str, Any] = {
                "id": _write_doc_id(checkpoint_id, task_id, idx),
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "task_id": task_id,
                "idx": idx,
                "channel": channel,
                "type": val_type,
                "value": base64.b64encode(val_bytes).decode(),
            }
            if task_path:
                doc["task_path"] = task_path
            await asyncio.to_thread(
                self._client.collections[WRITES_COLLECTION].documents.upsert, doc
            )

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        configurable = (config or {}).get("configurable", {})
        thread_id: Optional[str] = configurable.get("thread_id")
        checkpoint_ns: str = configurable.get("checkpoint_ns", "")

        filter_parts: list[str] = []
        if thread_id:
            filter_parts.append(f"thread_id:=`{_sanitize_filter_value(thread_id)}`")
            if checkpoint_ns:  # only add ns filter when non-empty (Typesense empty string bug)
                filter_parts.append(f"checkpoint_ns:=`{_sanitize_filter_value(checkpoint_ns)}`")

        if before:
            before_id = get_checkpoint_id(before)
            if before_id:
                try:
                    before_doc = await asyncio.to_thread(
                        self._client.collections[CHECKPOINTS_COLLECTION]
                        .documents[_checkpoint_doc_id(before_id)]
                        .retrieve
                    )
                    filter_parts.append(f"ts_ms:<{before_doc['ts_ms']}")
                except typesense.exceptions.ObjectNotFound:
                    return

        if limit is not None and limit <= 0:
            return

        filter_by = " && ".join(filter_parts) if filter_parts else None
        page = 1
        per_page = 250
        remaining = limit

        while True:
            this_page = per_page if remaining is None else min(per_page, remaining)
            params: dict[str, Any] = {
                "q": "*",
                "query_by": "thread_id",
                "per_page": this_page,
                "sort_by": "ts_ms:desc",
                "page": page,
            }
            if filter_by:
                params["filter_by"] = filter_by

            results = await asyncio.to_thread(
                self._client.collections[CHECKPOINTS_COLLECTION].documents.search,
                params,  # type: ignore[arg-type]
            )
            hits = results.get("hits", [])
            if not hits:
                break

            for hit in hits:
                doc = cast(dict[str, Any], hit["document"])
                if filter:
                    meta: CheckpointMetadata = self.serde.loads_typed(
                        (doc["metadata_type"], base64.b64decode(doc["metadata"]))
                    )
                    if not all(meta.get(k) == v for k, v in filter.items()):  # type: ignore[union-attr]
                        continue
                yield self._doc_to_tuple(doc, [])
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            if len(hits) < per_page:
                break
            page += 1

    async def adelete_thread(self, thread_id: str) -> None:
        for collection in [CHECKPOINTS_COLLECTION, WRITES_COLLECTION]:
            await asyncio.to_thread(
                self._client.collections[collection].documents.delete,
                {"filter_by": f"thread_id:=`{_sanitize_filter_value(thread_id)}`"},  # type: ignore[arg-type]
            )

    # Sync stubs — this checkpointer is async-only
    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        raise NotImplementedError("Use aget_tuple")

    def list(self, config: Any, **kwargs: Any) -> Iterator[CheckpointTuple]:
        raise NotImplementedError("Use alist")

    def put(self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any) -> RunnableConfig:
        raise NotImplementedError("Use aput")

    def put_writes(self, config: Any, writes: Any, task_id: Any, task_path: str = "") -> None:
        raise NotImplementedError("Use aput_writes")
