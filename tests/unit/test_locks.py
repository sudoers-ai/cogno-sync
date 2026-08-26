"""Mutual exclusion — the in-process default and the Redis adapter.

The Redis adapter is driven against a fake client that implements the same three commands a
real server does, so the OWNER-ONLY release is observable: the Lua compare-and-delete is the
whole reason this adapter is not two lines of ``SET``/``DEL``.
"""

from __future__ import annotations

import asyncio

import pytest

from cogno_sync import InProcessLockProvider, LockProvider, RedisLockProvider


# ── InProcessLockProvider ─────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_in_process_serializes_same_key():
    provider = InProcessLockProvider()
    trace: list[str] = []

    async def worker(tag: str) -> None:
        async with provider.lock("s1"):
            trace.append(f"enter-{tag}")
            await asyncio.sleep(0.01)            # yield so a race would interleave
            trace.append(f"exit-{tag}")

    await asyncio.gather(worker("a"), worker("b"))
    # mutual exclusion: each critical section completes before the next starts
    assert trace in (["enter-a", "exit-a", "enter-b", "exit-b"],
                     ["enter-b", "exit-b", "enter-a", "exit-a"])


@pytest.mark.asyncio
async def test_in_process_allows_different_keys_concurrently():
    provider = InProcessLockProvider()
    trace: list[str] = []

    async def worker(key: str) -> None:
        async with provider.lock(key):
            trace.append(f"enter-{key}")
            await asyncio.sleep(0.01)
            trace.append(f"exit-{key}")

    await asyncio.gather(worker("a"), worker("b"))
    assert trace[:2] == ["enter-a", "enter-b"]   # both entered before either exited


def test_both_providers_satisfy_the_port():
    assert isinstance(InProcessLockProvider(), LockProvider)
    assert isinstance(RedisLockProvider(url="redis://localhost:6379/0"), LockProvider)


# ── RedisLockProvider (fake client) ───────────────────────────────────────────────────
class FakeRedis:
    """The three commands the adapter uses, with the two failure modes it must survive.

    ``eval`` reads the SCRIPT it is handed instead of hard-coding what the script is supposed to
    do. That distinction is the difference between a control and a decoration: with the compare
    baked into the fake, deleting the compare from the shipped Lua leaves every test green — the
    double answers for the code under test. Here the guard clause below is what a server does:
    a script that does not compare ``GET`` against ``ARGV[1]`` deletes unconditionally, and the
    owner-only test then fails, which is the whole point of writing it.
    """

    def __init__(self, *, set_fails: bool = False, always_locked: bool = False) -> None:
        self.store: dict[str, str] = {}
        self.set_fails = set_fails
        self.always_locked = always_locked
        self.evals: list[tuple] = []

    async def set(self, key, value, nx=False, ex=None):
        if self.set_fails:
            raise RuntimeError("redis down")
        if self.always_locked:
            return None
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def eval(self, script, n, key, arg):
        self.evals.append((key, arg))
        compares_the_owner = 'GET' in script and 'ARGV[1]' in script and '==' in script
        if compares_the_owner and self.store.get(key) != arg:
            return 0                             # someone else owns it now — leave it alone
        return 1 if self.store.pop(key, None) is not None else 0


def test_redis_provider_requires_client_or_url():
    with pytest.raises(ValueError, match="redis client or a url"):
        RedisLockProvider()


def test_redis_provider_url_as_positional_is_url():
    """A URL in the CLIENT slot would only surface on the first ``.set()`` — and the fail-open
    path would swallow it forever, leaving the lock silently off. It is read as the url."""
    provider = RedisLockProvider("redis://localhost:6379/0")
    assert provider._client is None
    assert provider._url == "redis://localhost:6379/0"


@pytest.mark.asyncio
async def test_redis_acquire_and_owner_only_release():
    fake = FakeRedis()
    provider = RedisLockProvider(fake)
    async with provider.lock("s1"):
        assert "cogno:lock:s1" in fake.store          # held during the body
    assert "cogno:lock:s1" not in fake.store          # released after
    assert fake.evals and fake.evals[0][0] == "cogno:lock:s1"   # released via the Lua script


@pytest.mark.asyncio
async def test_release_NEVER_deletes_a_lock_taken_by_SOMEONE_ELSE():
    """THE test for this adapter, and the reason release is a script and not a ``DEL``.

    Our TTL expires while we are still working, a second worker takes the key, and then we
    finish: a bare ``DEL`` would delete THEIR lock and hand the key to a third worker while the
    second is still inside its critical section — mutual exclusion silently gone, under exactly
    the load that causes it.

    Mutation: replace the Lua compare-and-delete with an unconditional ``DEL`` and this dies.
    """
    fake = FakeRedis()
    provider = RedisLockProvider(fake)
    async with provider.lock("s1"):
        # the TTL expired and another worker took it — same key, a different owner's token
        fake.store["cogno:lock:s1"] = "someone-elses-token"
    assert fake.store["cogno:lock:s1"] == "someone-elses-token", (
        "the other worker still holds its lock")


@pytest.mark.asyncio
async def test_two_holders_of_the_same_key_cannot_overlap():
    """The property, over the fake server: NX refuses the second acquirer until the first
    releases. (The in-process default cannot show this — it shares memory.)"""
    fake = FakeRedis()
    a = RedisLockProvider(fake, retry_interval=0.01, max_wait=1.0)
    b = RedisLockProvider(fake, retry_interval=0.01, max_wait=1.0)
    trace: list[str] = []

    async def hold(provider, tag):
        async with provider.lock("s1"):
            trace.append(f"enter-{tag}")
            await asyncio.sleep(0.05)
            trace.append(f"exit-{tag}")

    await asyncio.gather(hold(a, "a"), hold(b, "b"))
    assert trace[0].startswith("enter") and trace[1].startswith("exit")


@pytest.mark.asyncio
async def test_redis_fail_open_on_error():
    provider = RedisLockProvider(FakeRedis(set_fails=True))
    ran = False
    async with provider.lock("s1"):
        ran = True                              # body still runs despite Redis being down
    assert ran


@pytest.mark.asyncio
async def test_redis_fail_open_on_acquire_timeout():
    fake = FakeRedis(always_locked=True)        # NX never succeeds → timeout
    provider = RedisLockProvider(fake, retry_interval=0.01, max_wait=0.05)
    ran = False
    async with provider.lock("s1"):
        ran = True                              # proceeds after the wait (fail-open)
    assert ran
    assert fake.evals == []                     # never acquired → never tries to release


@pytest.mark.asyncio
async def test_a_release_that_fails_does_not_break_the_caller():
    class _ReleaseFails(FakeRedis):
        async def eval(self, *a, **k):
            raise RuntimeError("redis went away between acquire and release")

    provider = RedisLockProvider(_ReleaseFails())
    async with provider.lock("s1"):
        pass                                    # must not raise out of the context manager


@pytest.mark.asyncio
async def test_the_key_prefix_is_configurable():
    fake = FakeRedis()
    provider = RedisLockProvider(fake, key_prefix="app:mutex:")
    async with provider.lock("s1"):
        assert "app:mutex:s1" in fake.store
