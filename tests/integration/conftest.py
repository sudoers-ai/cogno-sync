"""Real-Redis guard: these tests run only when ``COGNO_TEST_REDIS_URL`` points at a server.

They exist for the properties a fake cannot show — that two CLIENTS (two workers) share one
bucket, that a claim survives across connections, that the server really holds the TTL, and that
the release script compares the owner token inside Redis.
"""

from __future__ import annotations

import asyncio
import os

import pytest

pytest.importorskip("redis", reason="the [redis] extra")

REDIS_URL = os.environ.get("COGNO_TEST_REDIS_URL", "")


def _reachable(url: str) -> bool:
    async def ping() -> bool:
        from redis.asyncio import Redis
        client = Redis.from_url(url)
        try:
            await client.ping()
            return True
        except Exception:      # noqa: BLE001 — unreachable is the answer, not an error
            return False
        finally:
            await client.aclose()

    try:
        return asyncio.run(ping())
    except Exception:          # noqa: BLE001
        return False


@pytest.fixture(scope="session")
def redis_url() -> str:
    if not REDIS_URL:
        pytest.skip("set COGNO_TEST_REDIS_URL to run the real-Redis suite")
    if not _reachable(REDIS_URL):
        pytest.skip(f"no reachable Redis at {REDIS_URL}")
    return REDIS_URL
