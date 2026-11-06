"""Seat holds in Redis: atomic, self-expiring reservations."""
from __future__ import annotations

from enum import StrEnum

import redis.asyncio as redis

from app import scripts
from app.db import Event
from app.redis_conn import key


class HoldResult(StrEnum):
    HELD = "HELD"
    ALREADY_YOURS = "ALREADY_YOURS"
    TAKEN = "TAKEN"
    SOLD = "SOLD"
    LIMIT = "LIMIT"


class SeatStore:
    def __init__(self, r: redis.Redis):
        self.r = r
        self._hold = r.register_script(scripts.HOLD)
        self._release = r.register_script(scripts.RELEASE)
        self._claim = r.register_script(scripts.CLAIM)
        self._sold = r.register_script(scripts.MARK_SOLD)

    @staticmethod
    def _keys(event_id: str, seat_id: str, user_id: str) -> tuple[str, str, str]:
        return key(event_id, "seat", seat_id), key(event_id, "sold"), key(event_id, "holds", user_id)

    async def hold(self, event_id: str, seat_id: str, user_id: str, ttl_ms: int, max_holds: int) -> HoldResult:
        seat, sold, holds = self._keys(event_id, seat_id, user_id)
        out = await self._hold(keys=[seat, sold, holds], args=[user_id, ttl_ms, max_holds, seat_id])
        return HoldResult(out)

    async def release(self, event_id: str, seat_id: str, user_id: str) -> bool:
        seat, _, holds = self._keys(event_id, seat_id, user_id)
        return bool(await self._release(keys=[seat, holds], args=[user_id, seat_id]))

    async def claim(self, event_id: str, seat_id: str, user_id: str, grace_ms: int) -> int:
        """Remaining hold time in ms (extended to at least grace_ms), or -1 if the user does not hold it."""
        return int(await self._claim(keys=[key(event_id, "seat", seat_id)], args=[user_id, grace_ms]))

    async def mark_sold(self, event_id: str, seat_id: str, user_id: str) -> None:
        seat, sold, holds = self._keys(event_id, seat_id, user_id)
        await self._sold(keys=[seat, sold, holds], args=[seat_id])

    async def resync_sold(self, event_id: str, seat_ids: list[str]) -> None:
        """Rebuild Redis' sold set from Postgres (e.g. after a Redis restart or a crash between
        the database commit and mark_sold). Postgres wins."""
        if seat_ids:
            await self.r.sadd(key(event_id, "sold"), *seat_ids)

    async def seat_map(self, event: Event, user_id: str | None = None) -> dict[str, str]:
        """One round trip for the whole map: MGET every seat key plus the sold set."""
        ids = event.seat_ids()
        async with self.r.pipeline(transaction=False) as p:
            p.smembers(key(event.id, "sold"))
            p.mget([key(event.id, "seat", s) for s in ids])
            sold, owners = await p.execute()
        out = {}
        for seat_id, owner in zip(ids, owners):
            if seat_id in sold:
                out[seat_id] = "sold"
            elif owner is None:
                out[seat_id] = "free"
            else:
                out[seat_id] = "yours" if owner == user_id else "held"
        return out
