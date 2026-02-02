from __future__ import annotations

import io
import datetime as dt
import pandas as pd
import requests

from sqlalchemy import text
from pipelines.db import get_engine


DEFAULT_TICKERS = ["SPY", "QQQ", "IWM", "AGG", "TLT", "XLK", "XLF", "XLE", "GLD"]


def stooq_url(ticker: str) -> str:
    t = ticker.lower()
    return f"https://stooq.com/q/d/l/?s={t}.us&i=d"


def fetch_one_ticker(ticker: str) -> pd.DataFrame:
    url = stooq_url(ticker)
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.text))
    if df.empty:
        raise ValueError(f"no rows for {ticker}")

    df.columns = [c.strip().lower() for c in df.columns]
    expected = {"date", "open", "high", "low", "close", "volume"}
    if not expected.issubset(set(df.columns)):
        raise ValueError(f"unexpected columns for {ticker}: {df.columns.tolist()}")

    df = df.rename(columns={"date": "asof_date"})
    df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.date
    df["ticker"] = ticker.upper()
    df["source"] = "stooq"
    return df[["asof_date", "ticker", "open", "high", "low", "close", "volume", "source"]]


def upsert_prices(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    eng = get_engine()
    sql = text("""
    insert into raw.raw_prices
      (asof_date, ticker, open, high, low, close, volume, source)
    values
      (:asof_date, :ticker, :open, :high, :low, :close, :volume, :source)
    on conflict (asof_date, ticker)
    do update set
      open = excluded.open,
      high = excluded.high,
      low = excluded.low,
      close = excluded.close,
      volume = excluded.volume,
      source = excluded.source,
      ingested_at = now();
    """)
    rows = df.to_dict(orient="records")
    with eng.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def main(tickers: list[str] | None = None) -> None:
    tickers = tickers or DEFAULT_TICKERS
    frames = []
    for t in tickers:
        frames.append(fetch_one_ticker(t))
    all_df = pd.concat(frames, ignore_index=True)
    inserted = upsert_prices(all_df)
    latest = all_df["asof_date"].max()
    earliest = all_df["asof_date"].min()
    print(f"prices rows upserted: {inserted}")
    print(f"date range: {earliest} to {latest}")


if __name__ == "__main__":
    main()
