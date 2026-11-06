"""Everything a request needs, built once per process and shared."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.db import make_engine, make_sessionmaker
from app.payments import FakeGateway
from app.ratelimit import RateLimiter
from app.redis_conn import make_redis, warm_pool
from app.seats import SeatStore
from app.waiting_room import WaitingRoom


@dataclass
class Metrics:
    """In-process counters; exported in Prometheus format at /metrics."""
    counts: Counter = field(default_factory=Counter)

    def inc(self, name: str, n: int = 1) -> None:
        self.counts[name] += n


@dataclass
class Services:
    settings: Settings
    redis: redis.Redis
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    seats: SeatStore
    room: WaitingRoom
    limiter: RateLimiter
    gateway: FakeGateway
    metrics: Metrics

    @classmethod
    def build(cls, settings: Settings) -> "Services":
        r = make_redis(settings.redis_url, settings.redis_pool)
        engine = make_engine(settings.database_url)
        return cls(settings, r, engine, make_sessionmaker(engine), SeatStore(r), WaitingRoom(r),
                   RateLimiter(r, settings.rate_capacity, settings.rate_refill_per_sec), FakeGateway(r), Metrics())

    async def start(self) -> None:
        await warm_pool(self.redis, self.settings.redis_warm)

    async def close(self) -> None:
        await self.redis.aclose()
        await self.engine.dispose()
