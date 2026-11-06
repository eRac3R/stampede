"""Settings from environment variables. `.env.local` is read for local development only."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_LINE = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$")

LOCAL_REDIS = "redis://localhost:6380/0"
LOCAL_DB = "postgresql+psycopg://postgres:stampede@localhost:5433/stampede"


def load_dotenv() -> None:
    """Real environment variables always win. Tests set STAMPEDE_SKIP_DOTENV so they never see real secrets."""
    if os.getenv("STAMPEDE_SKIP_DOTENV"):
        return
    path = ROOT / ".env.local"
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        m = _LINE.match(raw)
        if m and not raw.lstrip().startswith("#"):
            os.environ.setdefault(m.group(1), m.group(2).strip().strip("'\""))


def normalize_db_url(url: str) -> str:
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix):]
    return url


@dataclass(frozen=True)
class Settings:
    redis_url: str = LOCAL_REDIS
    database_url: str = LOCAL_DB
    secret: str = "dev-only-secret-change-me"
    hold_seconds: int = 120          # how long a picked seat stays reserved
    checkout_grace_seconds: int = 30  # hold is extended by this when payment starts
    max_holds_per_user: int = 4
    rate_capacity: int = 20           # token bucket: burst size per client
    rate_refill_per_sec: float = 10.0
    redis_pool: int = 50              # max open Redis connections per process
    redis_warm: int = 8               # connections opened at startup

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        return cls(
            redis_url=os.getenv("STAMPEDE_REDIS_URL") or os.getenv("UPSTASH_REDIS_URL") or LOCAL_REDIS,
            database_url=normalize_db_url(os.getenv("DATABASE_URL") or LOCAL_DB),
            secret=os.getenv("STAMPEDE_SECRET", cls.secret),
            hold_seconds=int(os.getenv("STAMPEDE_HOLD_SECONDS", cls.hold_seconds)),
            max_holds_per_user=int(os.getenv("STAMPEDE_MAX_HOLDS", cls.max_holds_per_user)),
            rate_capacity=int(os.getenv("STAMPEDE_RATE_CAPACITY", cls.rate_capacity)),
            rate_refill_per_sec=float(os.getenv("STAMPEDE_RATE_REFILL", cls.rate_refill_per_sec)),
            redis_pool=int(os.getenv("STAMPEDE_REDIS_POOL", cls.redis_pool)),
            redis_warm=int(os.getenv("STAMPEDE_REDIS_WARM", cls.redis_warm)),
        )
