import os

import pytest
import pytest_asyncio
import typesense
from langgraph_checkpoint_typesense.saver import (
    AsyncTypesenseSaver,
    CHECKPOINTS_COLLECTION,
    WRITES_COLLECTION,
)

_TYPESENSE_API_KEY = os.environ.get("TYPESENSE_API_KEY", "test-api-key")


@pytest.fixture(scope="session")
def ts_client():
    return typesense.Client({
        "nodes": [{"host": "localhost", "port": "8108", "protocol": "http"}],
        "api_key": _TYPESENSE_API_KEY,
        "connection_timeout_seconds": 5,
    })


@pytest_asyncio.fixture
async def saver(ts_client):
    s = AsyncTypesenseSaver(ts_client)
    await s.setup()
    yield s
    # cleanup: drop collections after each test
    for col in [CHECKPOINTS_COLLECTION, WRITES_COLLECTION]:
        try:
            ts_client.collections[col].delete()
        except Exception:
            pass
