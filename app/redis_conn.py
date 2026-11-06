"""Redis connection and key naming."""
from __future__ import annotations

import asyncio

import certifi
import redis.asyncio as redis


def make_redis(url: str, max_connections: int = 50) -> redis.Redis:
    # Upstash requires TLS even when the dashboard shows a redis:// URL.
    if "upstash.io" in url and url.startswith("redis://"):
        url = "rediss://" + url[len("redis://"):]
    kwargs: dict = {"decode_responses": True}
    if url.startswith("rediss://"):
        # Windows' certificate store can hold an outdated Let's Encrypt chain; certifi's bundle is current.
        kwargs["ssl_ca_certs"] = certifi.where()
    # Bounded + blocking: under a spike, requests queue for a connection (backpressure) instead
    # of opening unlimited sockets or failing with "too many connections".
    pool = redis.BlockingConnectionPool.from_url(url, max_connections=max_connections, timeout=20,
                                                 socket_connect_timeout=15, health_check_interval=30, **kwargs)
    return redis.Redis(connection_pool=pool)


async def warm_pool(r: redis.Redis, n: int, batch: int = 4) -> None:
    """Open connections before traffic arrives. A TLS handshake to a hosted Redis costs ~0.4s
    while a command on an open connection costs ~30ms, so a cold pool would make the first
    burst of buyers wait on handshakes exactly when the sale opens."""
    for start in range(0, n, batch):
        await asyncio.gather(*[r.ping() for _ in range(min(batch, n - start))])


def key(event_id: str, *parts: str) -> str:
    """All keys of one event share a {hash tag}, so they land on the same Redis Cluster slot
    and multi-key Lua scripts stay legal if the deployment is ever sharded."""
    return f"{{ev:{event_id}}}:" + ":".join(parts)
