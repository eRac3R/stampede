"""A fake card processor that behaves like a real one where it matters: it is a separate system,
it can be slow, it can decline, and it is idempotent per key (charging twice with the same key
returns the first charge instead of taking money twice). State lives in Redis so every API
server sees the same charges.

Test cards: 4242 4242 4242 4242 succeeds, 4000 0000 0000 0002 is declined.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import asdict, dataclass

import redis.asyncio as redis

DECLINE_CARD = "4000000000000002"


@dataclass
class Charge:
    id: str
    status: str  # succeeded | declined | refunded
    amount_cents: int


class FakeGateway:
    def __init__(self, r: redis.Redis, latency_s: float = 0.0):
        self.r, self.latency_s = r, latency_s

    async def charge(self, idempotency_key: str, amount_cents: int, card: str) -> Charge:
        k = f"pay:{idempotency_key}"
        if cached := await self.r.get(k):
            return Charge(**json.loads(cached))
        if self.latency_s:
            await asyncio.sleep(self.latency_s)
        digits = "".join(ch for ch in card if ch.isdigit())
        status = "declined" if digits == DECLINE_CARD or len(digits) < 12 else "succeeded"
        charge = Charge(id="ch_" + uuid.uuid4().hex[:20], status=status, amount_cents=amount_cents)
        # NX: if two identical requests race, the first stored charge wins and both see it.
        await self.r.set(k, json.dumps(asdict(charge)), nx=True, ex=86400)
        return Charge(**json.loads(await self.r.get(k)))

    async def refund(self, idempotency_key: str) -> None:
        k = f"pay:{idempotency_key}"
        if cached := await self.r.get(k):
            await self.r.set(k, json.dumps(json.loads(cached) | {"status": "refunded"}), ex=86400)
