import asyncio

import pytest
from sqlalchemy import func, select

from app.checkout import CheckoutError, checkout
from app.db import Order, Outbox, Ticket
from app.payments import DECLINE_CARD

CARD = "4242424242424242"
TTL = 60_000


async def count(svc, model, *where):
    async with svc.sessions() as s:
        return (await s.execute(select(func.count()).select_from(model).where(*where))).scalar_one()


async def test_happy_path_writes_order_ticket_and_outbox_atomically(svc, event):
    await svc.seats.hold(event.id, "A1", "alice-1", TTL, 4)
    res = await checkout(svc, event, "alice-1", "A1", CARD, "key-alice-0001")
    assert res.seat_id == "A1" and not res.replayed
    assert await count(svc, Ticket) == 1 and await count(svc, Outbox) == 1
    assert (await svc.seats.seat_map(event))["A1"] == "sold"


async def test_retrying_with_the_same_key_charges_once(svc, event):
    await svc.seats.hold(event.id, "A1", "alice-1", TTL, 4)
    first = await checkout(svc, event, "alice-1", "A1", CARD, "key-alice-0001")
    again = await checkout(svc, event, "alice-1", "A1", CARD, "key-alice-0001")
    assert again.replayed and again.order_id == first.order_id
    assert await count(svc, Order) == 1


async def test_double_click_in_flight_only_one_proceeds(svc, event):
    svc.gateway.latency_s = 0.2  # keep the first request busy while the second arrives
    await svc.seats.hold(event.id, "A1", "alice-1", TTL, 4)
    results = await asyncio.gather(
        checkout(svc, event, "alice-1", "A1", CARD, "key-alice-0001"),
        checkout(svc, event, "alice-1", "A1", CARD, "key-alice-0001"),
        return_exceptions=True,
    )
    codes = sorted(r.code if isinstance(r, CheckoutError) else "OK" for r in results)
    assert codes == ["IN_PROGRESS", "OK"]
    assert await count(svc, Order) == 1


async def test_key_reused_by_another_user_is_rejected(svc, event):
    await svc.seats.hold(event.id, "A1", "alice-1", TTL, 4)
    await checkout(svc, event, "alice-1", "A1", CARD, "key-shared-01")
    with pytest.raises(CheckoutError) as e:
        await checkout(svc, event, "bob-123", "A1", CARD, "key-shared-01")
    assert e.value.code == "IDEMPOTENCY_KEY_REUSED"


async def test_no_hold_no_sale_and_declined_card_keeps_seat(svc, event):
    with pytest.raises(CheckoutError) as e:
        await checkout(svc, event, "alice-1", "A1", CARD, "key-alice-0001")
    assert e.value.code == "HOLD_EXPIRED"
    await svc.seats.hold(event.id, "A1", "alice-1", TTL, 4)
    with pytest.raises(CheckoutError) as e:
        await checkout(svc, event, "alice-1", "A1", DECLINE_CARD, "key-alice-0002")
    assert e.value.code == "PAYMENT_DECLINED"
    assert (await svc.seats.seat_map(event, "alice-1"))["A1"] == "yours"  # can retry with another card
    assert await count(svc, Ticket) == 0


async def test_database_is_the_last_line_of_defence(svc, event):
    """Simulate Redis losing state after a sale: Bob gets a hold on an already-sold seat.
    Postgres' unique constraint refuses the second ticket and Bob is refunded."""
    await svc.seats.hold(event.id, "A1", "alice-1", TTL, 4)
    await checkout(svc, event, "alice-1", "A1", CARD, "key-alice-0001")
    await svc.redis.flushdb()  # Redis forgets everything, including that A1 is sold

    await svc.seats.hold(event.id, "A1", "bob-123", TTL, 4)
    with pytest.raises(CheckoutError) as e:
        await checkout(svc, event, "bob-123", "A1", CARD, "key-bob-00001")
    assert e.value.code == "SEAT_ALREADY_SOLD"
    assert await count(svc, Ticket) == 1
    assert await count(svc, Order, Order.status == "refunded") == 1
    assert svc.metrics.counts["oversell_prevented_by_db"] == 1
    charge = await svc.gateway.charge("key-bob-00001", 0, CARD)  # replaying the key shows the refund
    assert charge.status == "refunded"
    with pytest.raises(CheckoutError) as e:  # a retry gets the same answer, no new charge
        await checkout(svc, event, "bob-123", "A1", CARD, "key-bob-00001")
    assert e.value.code == "SEAT_ALREADY_SOLD"


async def test_stampede_every_seat_sold_exactly_once(svc, event):
    """100 buyers, 10 seats, everyone tries every seat at once."""
    seats = event.seat_ids()

    async def buyer(i: int):
        user = f"buyer-{i:04d}"
        for seat in seats[i % len(seats):] + seats[: i % len(seats)]:
            if (await svc.seats.hold(event.id, seat, user, TTL, 1)).value == "HELD":
                try:
                    return await checkout(svc, event, user, seat, CARD, f"key-{user}")
                except CheckoutError:
                    return None
        return None

    sold = [r for r in await asyncio.gather(*[buyer(i) for i in range(100)]) if r]
    assert sorted(r.seat_id for r in sold) == sorted(seats)
    assert await count(svc, Ticket) == len(seats)
    assert await count(svc, Order, Order.status == "paid") == len(seats)
