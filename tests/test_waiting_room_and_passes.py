import asyncio

from app import passes
from app.events import create_event
from app.waiting_room import admitted_count


async def test_join_order_is_fair_and_rejoining_keeps_your_place(svc, event):
    ranks = [await svc.room.join(event.id, f"user-{i:04d}") for i in range(5)]
    assert ranks == [0, 1, 2, 3, 4]
    assert await svc.room.join(event.id, "user-0001") == 1
    assert await svc.room.size(event.id) == 5


async def test_concurrent_joins_get_unique_places(svc, event):
    ranks = await asyncio.gather(*[svc.room.join(event.id, f"user-{i:04d}") for i in range(300)])
    assert sorted(ranks) == list(range(300))


async def test_admission_drains_at_the_configured_rate(svc):
    ev = await create_event(svc, "slow-drop", "Slow", opens_in=0, admit_per_sec=2, admit_burst=3)
    for i in range(10):
        await svc.room.join(ev.id, f"user-{i:04d}")
    start = ev.sale_starts_at
    assert admitted_count(ev, start - 1) == 0
    assert admitted_count(ev, start) == 3
    assert admitted_count(ev, start + 2) == 7
    early = await svc.room.status(ev, "user-0005", now=start - 10)
    assert not early.admitted and not early.sale_open
    later = await svc.room.status(ev, "user-0005", now=start + 1)  # 5 admitted, user is 6th
    assert not later.admitted and later.ahead == 1 and later.eta_seconds == 0.5
    assert (await svc.room.status(ev, "user-0005", now=start + 1.5)).admitted


def test_pass_is_bound_to_user_event_secret_and_time():
    tok = passes.issue("s3cret", "drop", "alice-1", ttl_seconds=60, now=1000)
    assert passes.verify("s3cret", tok, "drop", "alice-1", now=1030)
    assert not passes.verify("s3cret", tok, "drop", "alice-1", now=1061)    # expired
    assert not passes.verify("s3cret", tok, "drop", "bob-123", now=1030)    # someone else's pass
    assert not passes.verify("s3cret", tok, "other", "alice-1", now=1030)   # other event
    assert not passes.verify("wrong", tok, "drop", "alice-1", now=1030)     # forged
    assert not passes.verify("s3cret", "garbage", "drop", "alice-1", now=1030)
