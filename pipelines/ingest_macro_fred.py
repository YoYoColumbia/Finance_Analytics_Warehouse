from __future__ import annotations

import datetime as dt
import pandas as pd
import requests
from sqlalchemy import text

from pipelines.config import Settings
from pipelines.db import get_engine


DEFAULT_SERIES = ["DGS10", "DGS2", "FEDFUNDS", "CPIAUCSL"]


def fetch_fred_series(series_id: str, api_key: str) -> pd.DataFrame:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    obs = data.get("observations", [])
    rows = []
    for o in obs:
        d = o.get("date")
        v = o.get("value")
        if d is None:
            continue
        if v in (None, ".", ""):
            val = None
        else:
            try:
                val = float(v)
            except ValueError:
                val = None
        rows.append({"asof_date": dt.date.fromisoformat(d), "series_id": series_id, "value": val, "source": "fred"})
    return pd.DataFrame(rows)


def upsert_macro(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    eng = get_engine()
    sql = text("""
    insert into raw.raw_macro
      (asof_date, series_id, value, source)
    values
      (:asof_date, :series_id, :value, :source)
    on conflict (asof_date, series_id)
    do update set
      value = excluded.value,
      source = excluded.source,
      ingested_at = now();
    """)
    rows = df.to_dict(orient="records")
    with eng.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def main(series: list[str] | None = None) -> None:
    s = Settings.from_env()
    if not s.fred_api_key:
        raise ValueError("FRED_API_KEY is missing in .env")

    series = series or DEFAULT_SERIES
    frames = []
    for sid in series:
        frames.append(fetch_fred_series(sid, s.fred_api_key))
    all_df = pd.concat(frames, ignore_index=True)
    inserted = upsert_macro(all_df)
    print(f"macro rows upserted: {inserted}")
    print(f"series: {', '.join(series)}")


if __name__ == "__main__":
    main()
