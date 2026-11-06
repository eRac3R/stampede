"""Tests use the local Docker Redis + Postgres (`docker compose up -d`), never the hosted ones."""
import asyncio
import os
import sys
import warnings

os.environ["STAMPEDE_SKIP_DOTENV"] = "1"  # before any app import: never read real secrets

if sys.platform == "win32":  # psycopg async cannot run on Windows' default Proactor loop
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import pytest
from httpx import ASGITransport, AsyncClient

from app.api import create_app
from app.config import Settings
from app.db import Base
from app.events import create_event
from app.services import Services

TEST_SETTINGS = Settings(
    redis_url=os.getenv("STAMPEDE_TEST_REDIS_URL", "redis://localhost:6380/15"),
    database_url=os.getenv("STAMPEDE_TEST_DATABASE_URL", "postgresql+psycopg://postgres:stampede@localhost:5433/stampede"),
    secret="test-secret",
    rate_capacity=10_000,
    rate_refill_per_sec=10_000,
)


@pytest.fixture
async def svc():
    s = Services.build(TEST_SETTINGS)
    await s.redis.flushdb()
    async with s.engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield s
    await s.close()


@pytest.fixture
async def event(svc):
    return await create_event(svc, "test-drop", "Test Drop", rows=2, cols=5, price_cents=1000,
                              opens_in=-1, admit_per_sec=1000, admit_burst=1000)


@pytest.fixture
async def client(svc):
    app = create_app(svc)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
