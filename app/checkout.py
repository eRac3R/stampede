"""Checkout: turn a seat hold into a paid ticket exactly once.

    1. replay      same Idempotency-Key seen before -> return the stored result, charge nothing
    2. lock        identical requests in flight at once -> only one proceeds
    3. claim       caller must still hold the seat; the hold is extended so it cannot lapse mid-payment
    4. pay         fake gateway, itself idempotent per key
    5. commit      order + ticket + outbox row in ONE Postgres transaction
                   (UNIQUE(event_id, seat_id) rejects a second sale -> refund)
    6. mark sold   update Redis; if this step is lost, resync_sold() repairs it from Postgres
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.db import Event, Order, Outbox, Ticket, new_id
from app.services import Services


class CheckoutError(Exception):
    def __init__(self, code: str, status: int, detail: str = ""):
        super().__init__(code)
        self.code, self.status, self.detail = code, status, detail or code


@dataclass
class CheckoutResult:
    order_id: str
    ticket_id: str
    seat_id: str
    amount_cents: int
    replayed: bool = False


async def _replay(svc: Services, idem_key: str, user_id: str) -> CheckoutResult | None:
    async with svc.sessions() as s:
        order = (await s.execute(select(Order).where(Order.idempotency_key == idem_key))).scalar_one_or_none()
        if order is None:
            return None
        if order.user_id != user_id:
            raise CheckoutError("IDEMPOTENCY_KEY_REUSED", 422, "this key belongs to another request")
        if order.status == "refunded":
            raise CheckoutError("SEAT_ALREADY_SOLD", 409, "seat was sold to someone else; you were refunded")
        ticket = (await s.execute(select(Ticket).where(Ticket.order_id == order.id))).scalar_one()
        return CheckoutResult(order.id, ticket.id, order.seat_id, order.amount_cents, replayed=True)


async def checkout(svc: Services, event: Event, user_id: str, seat_id: str, card: str, idem_key: str) -> CheckoutResult:
    if (done := await _replay(svc, idem_key, user_id)) is not None:
        return done

    lock = f"idem:{idem_key}"
    if not await svc.redis.set(lock, user_id, nx=True, ex=30):
        raise CheckoutError("IN_PROGRESS", 409, "an identical request is already being processed")
    try:
        grace_ms = svc.settings.checkout_grace_seconds * 1000
        if await svc.seats.claim(event.id, seat_id, user_id, grace_ms) < 0:
            raise CheckoutError("HOLD_EXPIRED", 409, "you no longer hold this seat")

        charge = await svc.gateway.charge(idem_key, event.price_cents, card)
        if charge.status != "succeeded":
            raise CheckoutError("PAYMENT_DECLINED", 402, "card declined")

        order_id, ticket_id = new_id(), new_id()
        try:
            async with svc.sessions() as s, s.begin():
                s.add(Order(id=order_id, event_id=event.id, user_id=user_id, seat_id=seat_id,
                            amount_cents=event.price_cents, status="paid", idempotency_key=idem_key, payment_id=charge.id))
                await s.flush()  # the order must exist before the ticket that references it
                s.add(Ticket(id=ticket_id, order_id=order_id, event_id=event.id, seat_id=seat_id))
                s.add(Outbox(topic="ticket.issued", payload={"ticket_id": ticket_id, "order_id": order_id,
                                                             "event_id": event.id, "seat_id": seat_id, "user_id": user_id}))
        except IntegrityError:
            # The database refused a second ticket for this seat. Money goes back, and the
            # refund is recorded under the same key so a retry gets the same answer.
            await svc.gateway.refund(idem_key)
            async with svc.sessions() as s, s.begin():
                s.add(Order(event_id=event.id, user_id=user_id, seat_id=seat_id, amount_cents=event.price_cents,
                            status="refunded", idempotency_key=idem_key, payment_id=charge.id))
            svc.metrics.inc("oversell_prevented_by_db")
            raise CheckoutError("SEAT_ALREADY_SOLD", 409, "seat was sold to someone else; you were refunded")

        await svc.seats.mark_sold(event.id, seat_id, user_id)
        svc.metrics.inc("tickets_sold")
        return CheckoutResult(order_id, ticket_id, seat_id, event.price_cents)
    finally:
        await svc.redis.delete(lock)
