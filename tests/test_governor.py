"""Core governor behavior: burst, queue timeout, cancel, coalesce, config swap."""

import asyncio
import logging

import pytest

from capacity_governor import (
    CapacityGovernor,
    GovernorConfig,
    OutcomeKind,
    RouteLimit,
)

logger = logging.getLogger("test.governor")


def make_governor(**limit_kw) -> CapacityGovernor:
    limit = RouteLimit(**limit_kw)
    return CapacityGovernor(GovernorConfig(default_limit=limit))


def log_outcomes(tag: str, outcomes) -> None:
    for i, o in enumerate(outcomes):
        logger.info(
            "%s req=%s-%d kind=%s reason=%s shared=%s queue_wait=%.4fs execute=%.4fs",
            tag, tag, i, o.kind.value, o.reason, o.shared,
            o.queue_wait_s, o.execute_s,
        )


async def test_burst_concurrency_rejects_excess_and_recovers(caplog):
    """Burst: 2 run + 2 queue + 2 rejected(queue_full); lane recovers after."""
    caplog.set_level(logging.INFO, logger="capacity_governor")
    gov = make_governor(max_concurrent=2, max_queued=2, queue_timeout_s=1.0,
                        coalesce_enabled=False)
    gate = asyncio.Event()

    async def slow():
        await gate.wait()
        return "done"

    tasks = [asyncio.create_task(gov.execute("/r", "c", slow, request_id=f"b{i}"))
             for i in range(6)]
    await asyncio.sleep(0.05)
    assert gov.lane_stats()["/r:c"] == {"running": 2, "waiting": 2}
    gate.set()
    outcomes = await asyncio.gather(*tasks)
    log_outcomes("burst", outcomes)

    kinds = [o.kind for o in outcomes]
    assert kinds.count(OutcomeKind.SUCCESS) == 4
    assert kinds.count(OutcomeKind.REJECTED) == 2
    assert all(o.reason == "queue_full" for o in outcomes
               if o.kind is OutcomeKind.REJECTED)
    # queued requests report real queue latency
    assert any(o.queue_wait_s > 0.01 for o in outcomes
               if o.kind is OutcomeKind.SUCCESS)

    # pressure released: normal requests succeed again
    outcome = await gov.execute("/r", "c", slow, request_id="recovery")
    assert outcome.kind is OutcomeKind.SUCCESS
    assert gov.lane_stats()["/r:c"] == {"running": 0, "waiting": 0}

    snap = gov.metrics.snapshot()["counters"]
    assert snap["rejected.queue_full"] == 2
    assert snap["admitted"] == 5
    assert any("decision=" in r.message for r in caplog.records)


async def test_queue_timeout_is_distinguishable(caplog):
    caplog.set_level(logging.INFO, logger="capacity_governor")
    gov = make_governor(max_concurrent=1, max_queued=1, queue_timeout_s=0.05,
                        coalesce_enabled=False)
    gate = asyncio.Event()

    async def slow():
        await gate.wait()

    holder = asyncio.create_task(gov.execute("/r", "c", slow, request_id="holder"))
    await asyncio.sleep(0.02)
    outcome = await gov.execute("/r", "c", slow, request_id="timeout-victim")
    log_outcomes("timeout", [outcome])
    assert outcome.kind is OutcomeKind.REJECTED
    assert outcome.reason == "queue_timeout"
    gate.set()
    await holder
    assert gov.metrics.snapshot()["counters"]["rejected.queue_timeout"] == 1
    assert gov.lane_stats()["/r:c"] == {"running": 0, "waiting": 0}


async def test_degraded_fallback_on_rejection(caplog):
    caplog.set_level(logging.INFO, logger="capacity_governor")
    gov = make_governor(max_concurrent=1, max_queued=0, queue_timeout_s=0.05,
                        coalesce_enabled=False)
    gate = asyncio.Event()

    async def slow():
        await gate.wait()

    async def fallback():
        return "fallback-payload"

    holder = asyncio.create_task(gov.execute("/r", "c", slow, request_id="holder"))
    await asyncio.sleep(0.02)
    outcome = await gov.execute("/r", "c", slow, request_id="deg",
                                degraded=fallback)
    log_outcomes("degraded", [outcome])
    assert outcome.kind is OutcomeKind.DEGRADED
    assert outcome.value == "fallback-payload"
    assert outcome.reason == "queue_full"
    gate.set()
    await holder
    assert gov.metrics.snapshot()["counters"]["degraded"] == 1


async def test_client_cancel_while_queued(caplog):
    caplog.set_level(logging.INFO, logger="capacity_governor")
    gov = make_governor(max_concurrent=1, max_queued=2, queue_timeout_s=5.0,
                        coalesce_enabled=False)
    gate = asyncio.Event()

    async def slow():
        await gate.wait()

    holder = asyncio.create_task(gov.execute("/r", "c", slow, request_id="holder"))
    await asyncio.sleep(0.02)
    victim = asyncio.create_task(gov.execute("/r", "c", slow, request_id="cancelled"))
    await asyncio.sleep(0.02)
    victim.cancel()
    outcome = await victim
    log_outcomes("cancel", [outcome])
    assert outcome.kind is OutcomeKind.CANCELLED
    assert outcome.reason == "cancelled"
    gate.set()
    await holder
    # no count drift after cancellation
    assert gov.lane_stats()["/r:c"] == {"running": 0, "waiting": 0}
    assert gov.metrics.snapshot()["counters"]["cancelled"] == 1


async def test_coalescing_merges_and_never_returns_early(caplog):
    caplog.set_level(logging.INFO, logger="capacity_governor")
    gov = make_governor(max_concurrent=4, max_queued=4, queue_timeout_s=1.0,
                        coalesce_enabled=True, coalesce_ttl_s=0.2)
    calls = 0
    entered = asyncio.Event()
    finish = asyncio.Event()

    async def work():
        nonlocal calls
        calls += 1
        entered.set()
        await finish.wait()
        return "v1"

    tasks = [asyncio.create_task(gov.execute("/r", "c", work, request_id=f"m{i}",
                                             coalesce_key="k"))
             for i in range(4)]
    await entered.wait()
    await asyncio.sleep(0.02)
    # result not produced yet: nobody may have returned
    assert all(not t.done() for t in tasks)
    finish.set()
    outcomes = await asyncio.gather(*tasks)
    log_outcomes("coalesce", outcomes)
    assert calls == 1, "identical requests must execute once"
    assert sum(o.shared for o in outcomes) == 3
    assert all(o.kind is OutcomeKind.SUCCESS and o.value == "v1" for o in outcomes)

    # TTL cache hit: no new execution
    cached = await gov.execute("/r", "c", work, request_id="ttl", coalesce_key="k")
    assert cached.shared and cached.value == "v1"
    assert calls == 1

    # expiry: re-executes
    await asyncio.sleep(0.25)
    finish.set()
    fresh = await gov.execute("/r", "c", work, request_id="fresh", coalesce_key="k")
    assert not fresh.shared
    assert calls == 2

    snap = gov.metrics.snapshot()["counters"]
    assert snap["coalesced.shared"] == 4
    assert snap["executed"] == 2


async def test_failed_execution_does_not_pollute():
    gov = make_governor(max_concurrent=2, max_queued=2, queue_timeout_s=1.0,
                        coalesce_enabled=True, coalesce_ttl_s=0.2)

    async def boom():
        raise RuntimeError("downstream broken")

    with pytest.raises(RuntimeError):
        await gov.execute("/r", "c", boom, request_id="f1", coalesce_key="bad")

    async def healthy():
        return "recovered"

    outcome = await gov.execute("/r", "c", healthy, request_id="f2",
                                coalesce_key="bad")
    assert outcome.kind is OutcomeKind.SUCCESS
    assert outcome.value == "recovered"
    assert gov.metrics.snapshot()["counters"]["errors"] == 1


async def test_config_swap_keeps_inflight_snapshot(caplog):
    """Config update mid-flight: no double execution, no count drift, no
    semantic mutation for the in-flight request."""
    caplog.set_level(logging.INFO, logger="capacity_governor")
    gov = make_governor(max_concurrent=2, max_queued=2, queue_timeout_s=1.0,
                        coalesce_enabled=False)
    gate = asyncio.Event()
    calls = 0

    async def slow():
        nonlocal calls
        calls += 1
        await gate.wait()
        return "done"

    inflight = asyncio.create_task(gov.execute("/r", "c", slow, request_id="swap"))
    await asyncio.sleep(0.02)
    old_version = gov.config.version
    gov.update_config(gov.config.with_updates(
        default_limit=RouteLimit(max_concurrent=1, max_queued=0,
                                 queue_timeout_s=0.01, coalesce_enabled=False)))
    assert gov.config.version == old_version + 1
    gate.set()
    outcome = await inflight
    log_outcomes("swap", [outcome])
    assert outcome.kind is OutcomeKind.SUCCESS
    assert outcome.config_version == old_version  # admitted under old snapshot
    assert calls == 1  # no duplicate execution

    # new requests see the new semantics: max_queued=0 -> immediate rejection
    gate.clear()
    holder = asyncio.create_task(gov.execute("/r", "c", slow, request_id="new1"))
    await asyncio.sleep(0.02)
    rejected = await gov.execute("/r", "c", slow, request_id="new2")
    assert rejected.kind is OutcomeKind.REJECTED
    assert rejected.reason == "queue_full"
    gate.set()
    await holder
    assert gov.lane_stats()["/r:c"] == {"running": 0, "waiting": 0}
