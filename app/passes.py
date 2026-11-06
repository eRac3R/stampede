"""Admission passes: signed, expiring tokens proving a user made it through the waiting room.

Stateless: any API server verifies a pass with the shared secret and no Redis lookup, so the
hot seat-picking path does not pay for an extra round trip.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import time


def _sign(secret: str, payload: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]


def issue(secret: str, event_id: str, user_id: str, ttl_seconds: int = 900, now: float | None = None) -> str:
    exp = int((time.time() if now is None else now) + ttl_seconds)
    payload = f"{event_id}|{user_id}|{exp}"
    body = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
    return f"{body}.{_sign(secret, payload)}"


def verify(secret: str, token: str, event_id: str, user_id: str, now: float | None = None) -> bool:
    try:
        body, sig = token.split(".", 1)
        payload = base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)).decode()
        ev, user, exp = payload.split("|")
    except (ValueError, UnicodeDecodeError):
        return False
    if not hmac.compare_digest(sig, _sign(secret, payload)):
        return False
    return ev == event_id and user == user_id and int(exp) > (time.time() if now is None else now)
