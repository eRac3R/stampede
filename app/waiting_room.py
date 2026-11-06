"""Virtual waiting room.

Everyone who arrives gets a strictly increasing ticket number (fair: first come, first served).
Admission is a pure function of time: `burst + rate * seconds_since_open` people are let in.
No background job has to run to move the line forward, which suits serverless hosts, and every
API server computes the same answer from the same Redis data.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass

import redis.asyncio as redis

from app import scripts
from app.db import Event
from app.redis_conn import key


@dataclass
class QueueStatus:
    joined: bool
    admitted: bool
    rank: int | None        # 0-based place in line
    ahead: int              # people still in front of you before you are let in
    sale_open: bool
    eta_seconds: float | None


def admitted_count(event: Event, now: float) -> int:
    if now < event.sale_starts_at:
        return 0
    return event.admit_burst + math.floor((now - event.sale_starts_at) * event.admit_per_sec)


class WaitingRoom:
    def __init__(self, r: redis.Redis):
        self.r = r
        self._join = r.register_script(scripts.JOIN)

    async def join(self, event_id: str, user_id: str) -> int:
        return int(await self._join(keys=[key(event_id, "queue"), key(event_id, "qseq")], args=[user_id]))

    async def size(self, event_id: str) -> int:
        return int(await self.r.zcard(key(event_id, "queue")))

    async def status(self, event: Event, user_id: str, now: float | None = None) -> QueueStatus:
        now = time.time() if now is None else now
        rank = await self.r.zrank(key(event.id, "queue"), user_id)
        is_open = now >= event.sale_starts_at
        if rank is None:
            return QueueStatus(False, False, None, 0, is_open, None)
        let_in = admitted_count(event, now)
        admitted = is_open and rank < let_in
        ahead = 0 if admitted else rank - let_in + 1
        if admitted:
            eta = 0.0
        elif not is_open:
            eta = (event.sale_starts_at - now) + max(0, rank - event.admit_burst + 1) / event.admit_per_sec
        else:
            eta = ahead / event.admit_per_sec
        return QueueStatus(True, admitted, int(rank), ahead, is_open, round(eta, 1))
