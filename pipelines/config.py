from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    fred_api_key: str | None

    @staticmethod
    def from_env() -> "Settings":
        host = os.getenv("PG_HOST", "localhost").strip()
        port_raw = os.getenv("PG_PORT", "5432").strip()
        db = os.getenv("PG_DB", "finance_dw").strip()
        user = os.getenv("PG_USER", "postgres").strip()
        pwd = os.getenv("PG_PASSWORD", "").strip()
        fred_key = os.getenv("FRED_API_KEY")
        if fred_key is not None:
            fred_key = fred_key.strip()

        if not pwd:
            raise ValueError("PG_PASSWORD is empty. Please set it in .env")

        return Settings(
            pg_host=host,
            pg_port=int(port_raw),
            pg_db=db,
            pg_user=user,
            pg_password=pwd,
            fred_api_key=fred_key,
        )
