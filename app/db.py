"""Postgres is the source of truth; Redis is the fast path.

The UNIQUE constraint on tickets(event_id, seat_id) is the last line of defence: even if
Redis state were lost or a hold expired mid-payment, the database refuses a second sale.
"""
from __future__ import annotations

import time
import uuid

from sqlalchemy import JSON, Float, ForeignKey, Integer, String, UniqueConstraint, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def new_id() -> str:
    return uuid.uuid4().hex


class Base(DeclarativeBase):
    pass


class Event(Base):
    __tablename__ = "events"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    venue: Mapped[str] = mapped_column(String(200), default="")
    rows: Mapped[int] = mapped_column(Integer)
    cols: Mapped[int] = mapped_column(Integer)
    price_cents: Mapped[int] = mapped_column(Integer)
    sale_starts_at: Mapped[float] = mapped_column(Float)
    admit_per_sec: Mapped[float] = mapped_column(Float, default=5.0)  # waiting-room drain rate
    admit_burst: Mapped[int] = mapped_column(Integer, default=10)     # admitted the instant the sale opens
    created_at: Mapped[float] = mapped_column(Float, default=time.time)

    def seat_ids(self) -> list[str]:
        return [f"{chr(65 + r)}{c + 1}" for r in range(self.rows) for c in range(self.cols)]

    def has_seat(self, seat_id: str) -> bool:
        if len(seat_id) < 2 or not seat_id[1:].isdigit():
            return False
        return 0 <= ord(seat_id[0]) - 65 < self.rows and 1 <= int(seat_id[1:]) <= self.cols


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    event_id: Mapped[str] = mapped_column(ForeignKey("events.id"), index=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    seat_id: Mapped[str] = mapped_column(String(8))
    amount_cents: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16))  # paid | refunded
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True)
    payment_id: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[float] = mapped_column(Float, default=time.time)


class Ticket(Base):
    __tablename__ = "tickets"
    __table_args__ = (UniqueConstraint("event_id", "seat_id", name="uq_one_ticket_per_seat"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"), unique=True)
    event_id: Mapped[str] = mapped_column(ForeignKey("events.id"))
    seat_id: Mapped[str] = mapped_column(String(8))
    created_at: Mapped[float] = mapped_column(Float, default=time.time)


class Outbox(Base):
    """Transactional outbox: written in the SAME transaction as the sale, published later.
    A crash can delay the 'ticket issued' message but can never lose it or send it for a sale
    that rolled back."""
    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[float] = mapped_column(Float, default=time.time)
    published_at: Mapped[float | None] = mapped_column(Float, nullable=True, index=True)


def make_engine(url: str) -> AsyncEngine:
    connect_args = {}
    if url.startswith("postgresql"):
        # Neon's pooled endpoint is PgBouncer in transaction mode: server-side prepared
        # statements do not survive across transactions there, so turn them off.
        connect_args["prepare_threshold"] = None
    return create_async_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10, connect_args=connect_args)


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_event(session: AsyncSession, event_id: str) -> Event | None:
    return await session.get(Event, event_id)


async def sold_seats(session: AsyncSession, event_id: str) -> list[str]:
    rows = await session.execute(select(Ticket.seat_id).where(Ticket.event_id == event_id))
    return list(rows.scalars())
