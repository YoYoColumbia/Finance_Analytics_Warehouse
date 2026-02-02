from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from pipelines.config import Settings


_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is not None:
        return _engine

    s = Settings.from_env()
    url = (
        f"postgresql+psycopg2://{s.pg_user}:{s.pg_password}"
        f"@{s.pg_host}:{s.pg_port}/{s.pg_db}"
    )
    _engine = create_engine(url, pool_pre_ping=True)
    return _engine


def run_sql(sql: str) -> None:
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(sql))


def fetch_one(sql: str) -> tuple | None:
    eng = get_engine()
    with eng.begin() as conn:
        res = conn.execute(text(sql)).fetchone()
        return None if res is None else tuple(res)
