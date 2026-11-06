"""HTTP API. Clients identify with an anonymous X-User-Id they generate themselves (no accounts
in the demo); the waiting room hands out signed admission passes that seat picking requires."""
from __future__ import annotations

import re
from contextlib import asynccontextmanager
from dataclasses import asdict

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from app import passes
from app.checkout import CheckoutError, checkout
from app.config import Settings
from app.db import Event, create_schema, get_event, sold_seats
from app.seats import HoldResult
from app.services import Services

USER_ID = re.compile(r"^[A-Za-z0-9_-]{6,64}$")


class HoldBody(BaseModel):
    seat_id: str = Field(max_length=8)


class CheckoutBody(BaseModel):
    seat_id: str = Field(max_length=8)
    card: str = Field(max_length=32)


HOLD_ERRORS = {
    HoldResult.TAKEN: (409, "someone else is holding this seat"),
    HoldResult.SOLD: (409, "seat already sold"),
    HoldResult.LIMIT: (429, "you are holding the maximum number of seats"),
}


def create_app(services: Services | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if services is None:
            svc = Services.build(Settings.from_env())
            await create_schema(svc.engine)
            await svc.start()
            app.state.svc = svc
            yield
            await svc.close()
        else:
            yield

    app = FastAPI(title="Stampede", version="0.1.0", lifespan=lifespan,
                  description="Flash-sale ticketing that never sells a seat twice.")
    if services is not None:
        app.state.svc = services

    def svc_dep(request: Request) -> Services:
        return request.app.state.svc

    def user_dep(x_user_id: str = Header()) -> str:
        if not USER_ID.match(x_user_id):
            raise HTTPException(400, "X-User-Id must be 6-64 chars of letters, digits, _ or -")
        return x_user_id

    async def limited(request: Request, svc: Services = Depends(svc_dep)) -> None:
        client = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or (
            request.client.host if request.client else "unknown")
        if not await svc.limiter.allow(client):
            svc.metrics.inc("rate_limited")
            raise HTTPException(429, "slow down", headers={"Retry-After": "1"})

    async def event_dep(event_id: str, svc: Services = Depends(svc_dep)) -> Event:
        async with svc.sessions() as s:
            event = await get_event(s, event_id)
        if event is None:
            raise HTTPException(404, "event not found")
        return event

    def require_pass(event: Event, user: str, svc: Services, token: str | None) -> None:
        if not token or not passes.verify(svc.settings.secret, token, event.id, user):
            raise HTTPException(403, "join the waiting room and wait for your turn first")

    @app.get("/health")
    async def health(svc: Services = Depends(svc_dep)):
        await svc.redis.ping()
        return {"status": "ok"}

    @app.get("/events/{event_id}")
    async def event_info(event: Event = Depends(event_dep), svc: Services = Depends(svc_dep),
                         x_user_id: str | None = Header(default=None)):
        seat_map = await svc.seats.seat_map(event, x_user_id)
        counts = {state: list(seat_map.values()).count(state) for state in ("free", "held", "yours", "sold")}
        return {"id": event.id, "name": event.name, "venue": event.venue, "rows": event.rows, "cols": event.cols,
                "price_cents": event.price_cents, "sale_starts_at": event.sale_starts_at,
                "in_line": await svc.room.size(event.id), "counts": counts, "seats": seat_map}

    @app.post("/events/{event_id}/queue", dependencies=[Depends(limited)])
    async def join_queue(event: Event = Depends(event_dep), user: str = Depends(user_dep),
                         svc: Services = Depends(svc_dep)):
        await svc.room.join(event.id, user)
        return await queue_status(event, user, svc)

    @app.get("/events/{event_id}/queue")
    async def queue_status(event: Event = Depends(event_dep), user: str = Depends(user_dep),
                           svc: Services = Depends(svc_dep)):
        status = await svc.room.status(event, user)
        body = asdict(status)
        if status.admitted:
            body["pass"] = passes.issue(svc.settings.secret, event.id, user)
        return body

    @app.post("/events/{event_id}/holds", dependencies=[Depends(limited)])
    async def hold(body: HoldBody, event: Event = Depends(event_dep), user: str = Depends(user_dep),
                   svc: Services = Depends(svc_dep), x_admission_pass: str | None = Header(default=None)):
        require_pass(event, user, svc, x_admission_pass)
        if not event.has_seat(body.seat_id):
            raise HTTPException(404, "no such seat")
        result = await svc.seats.hold(event.id, body.seat_id, user, svc.settings.hold_seconds * 1000,
                                      svc.settings.max_holds_per_user)
        svc.metrics.inc(f"hold_{result.value.lower()}")
        if result in HOLD_ERRORS:
            code, msg = HOLD_ERRORS[result]
            raise HTTPException(code, {"code": result.value, "message": msg})
        return {"seat_id": body.seat_id, "status": result.value, "hold_seconds": svc.settings.hold_seconds}

    @app.delete("/events/{event_id}/holds/{seat_id}")
    async def release(seat_id: str, event: Event = Depends(event_dep), user: str = Depends(user_dep),
                      svc: Services = Depends(svc_dep)):
        return {"released": await svc.seats.release(event.id, seat_id, user)}

    @app.post("/events/{event_id}/checkout", dependencies=[Depends(limited)])
    async def buy(body: CheckoutBody, event: Event = Depends(event_dep), user: str = Depends(user_dep),
                  svc: Services = Depends(svc_dep), idempotency_key: str = Header(min_length=8, max_length=128),
                  x_admission_pass: str | None = Header(default=None)):
        require_pass(event, user, svc, x_admission_pass)
        try:
            result = await checkout(svc, event, user, body.seat_id, body.card, idempotency_key)
        except CheckoutError as e:
            raise HTTPException(e.status, {"code": e.code, "message": e.detail})
        return asdict(result)

    @app.post("/events/{event_id}/resync")
    async def resync(event: Event = Depends(event_dep), svc: Services = Depends(svc_dep)):
        """Repair Redis from Postgres, the source of truth."""
        async with svc.sessions() as s:
            seats = await sold_seats(s, event.id)
        await svc.seats.resync_sold(event.id, seats)
        return {"sold": len(seats)}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics(svc: Services = Depends(svc_dep)):
        return "".join(f"stampede_{name}_total {n}\n" for name, n in sorted(svc.metrics.counts.items()))

    return app
