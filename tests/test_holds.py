import asyncio

from app.seats import HoldResult

TTL = 60_000


async def test_two_hundred_buyers_race_for_one_seat_exactly_one_wins(svc, event):
    results = await asyncio.gather(*[
        svc.seats.hold(event.id, "A1", f"user-{i:04d}", TTL, 4) for i in range(200)
    ])
    assert results.count(HoldResult.HELD) == 1
    assert results.count(HoldResult.TAKEN) == 199


async def test_hold_is_idempotent_for_its_owner_and_can_be_released(svc, event):
    assert await svc.seats.hold(event.id, "A1", "alice-1", TTL, 4) == HoldResult.HELD
    assert await svc.seats.hold(event.id, "A1", "alice-1", TTL, 4) == HoldResult.ALREADY_YOURS
    assert not await svc.seats.release(event.id, "A1", "mallory")  # cannot release someone else's hold
    assert await svc.seats.release(event.id, "A1", "alice-1")
    assert await svc.seats.hold(event.id, "A1", "bob-123", TTL, 4) == HoldResult.HELD


async def test_hold_expires_by_itself(svc, event):
    assert await svc.seats.hold(event.id, "A1", "alice-1", 150, 4) == HoldResult.HELD
    await asyncio.sleep(0.3)
    assert await svc.seats.hold(event.id, "A1", "bob-123", TTL, 4) == HoldResult.HELD


async def test_per_user_hold_limit_and_expired_holds_do_not_count(svc, event):
    for seat in ("A1", "A2"):
        assert await svc.seats.hold(event.id, seat, "alice-1", 150, 2) == HoldResult.HELD
    assert await svc.seats.hold(event.id, "A3", "alice-1", TTL, 2) == HoldResult.LIMIT
    await asyncio.sleep(0.3)
    assert await svc.seats.hold(event.id, "A3", "alice-1", TTL, 2) == HoldResult.HELD


async def test_sold_seat_cannot_be_held_and_map_shows_states(svc, event):
    await svc.seats.hold(event.id, "A1", "alice-1", TTL, 4)
    await svc.seats.mark_sold(event.id, "A1", "alice-1")
    await svc.seats.hold(event.id, "A2", "bob-123", TTL, 4)
    assert await svc.seats.hold(event.id, "A1", "carol-1", TTL, 4) == HoldResult.SOLD
    seat_map = await svc.seats.seat_map(event, "bob-123")
    assert seat_map["A1"] == "sold" and seat_map["A2"] == "yours" and seat_map["A3"] == "free"
    assert (await svc.seats.seat_map(event, "carol-1"))["A2"] == "held"


async def test_claim_extends_a_nearly_expired_hold(svc, event):
    await svc.seats.hold(event.id, "A1", "alice-1", 100, 4)
    assert await svc.seats.claim(event.id, "A1", "alice-1", 5_000) >= 4_000
    await asyncio.sleep(0.3)  # would have expired without the claim
    assert await svc.seats.claim(event.id, "A1", "bob-123", 5_000) == -1
    assert (await svc.seats.seat_map(event, "alice-1"))["A1"] == "yours"
