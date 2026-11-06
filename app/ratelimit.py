"""Token-bucket rate limiting in Redis, shared by every API server."""
from __future__ import annotations

import redis.asyncio as redis

from app import scripts


class RateLimiter:
    def __init__(self, r: redis.Redis, capacity: int, refill_per_sec: float):
        self.capacity, self.refill = capacity, refill_per_sec
        self._bucket = r.register_script(scripts.TOKEN_BUCKET)

    async def allow(self, client: str, cost: int = 1) -> bool:
        ok, _ = await self._bucket(keys=[f"rl:{client}"], args=[self.capacity, self.refill, cost])
        return bool(int(ok))
