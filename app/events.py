"""Creating and resetting events (used by the demo, the CLI and tests)."""
from __future__ import annotations

import time

from sqlalchemy import delete

from app.db import Event, Order, Outbox, Ticket
from app.redis_conn import key
from app.services import Services


async def clear_redis(svc: Services, event_id: str) -> None:
    async for k in svc.redis.scan_iter(match=key(event_id, "*").replace("{", "\\{").replace("}", "\\}"), count=500):
        await svc.redis.delete(k)


async def create_event(svc: Services, event_id: str, name: str, rows: int = 10, cols: int = 10,
                       price_cents: int = 4900, opens_in: float = 0.0, admit_per_sec: float = 5.0,
                       admit_burst: int = 10, venue: str = "") -> Event:
    """Create (or reset) an event: wipes its previous sales so a demo can be replayed."""
    if not (1 <= rows <= 26 and 1 <= cols <= 40):
        raise ValueError("rows must be 1-26 and cols 1-40")
    async with svc.sessions() as s, s.begin():
        await s.execute(delete(Ticket).where(Ticket.event_id == event_id))
        await s.execute(delete(Order).where(Order.event_id == event_id))
        await s.execute(delete(Outbox).where(Outbox.payload["event_id"].as_string() == event_id))
        await s.execute(delete(Event).where(Event.id == event_id))
        event = Event(id=event_id, name=name, venue=venue, rows=rows, cols=cols, price_cents=price_cents,
                      sale_starts_at=time.time() + opens_in, admit_per_sec=admit_per_sec, admit_burst=admit_burst)
        s.add(event)
    await clear_redis(svc, event_id)
    return event
