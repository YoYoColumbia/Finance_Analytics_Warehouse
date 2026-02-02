from __future__ import annotations

import io
import zipfile
import pandas as pd
import requests
from sqlalchemy import text

from pipelines.db import get_engine


FF3_DAILY_ZIP_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip"


def fetch_ff3_daily() -> pd.DataFrame:
    r = requests.get(FF3_DAILY_ZIP_URL, timeout=60)
    r.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(r.content))
    names = z.namelist()
    if not names:
        raise ValueError("ff3 zip is empty")

    with z.open(names[0]) as f:
        raw = f.read().decode("utf-8", errors="ignore")

    lines = raw.splitlines()

    header_idx = None
    for i, line in enumerate(lines):
        s = line.strip().lower().replace(" ", "")
        if ("mkt-rf" in s or "mktrf" in s) and ("smb" in s) and ("hml" in s) and (",rf" in s or "rf" in s):
            header_idx = i
            break

    if header_idx is None:
        preview = "\n".join(lines[:40])
        raise ValueError("cannot locate ff3 header. First lines:\n" + preview)

    data_lines = []
    for line in lines[header_idx + 1:]:
        s = line.strip()
        if not s:
            continue
        first = s.split(",")[0].strip()
        if first.isdigit() and len(first) == 8:
            data_lines.append(s)
        else:
            if len(data_lines) > 0:
                break

    if not data_lines:
        raise ValueError("ff3 data section not found after header")

    header_line = lines[header_idx].strip()
    csv_text = header_line + "\n" + "\n".join(data_lines)

    df = pd.read_csv(io.StringIO(csv_text))
    df.columns = [c.strip().lower() for c in df.columns]

    if "date" not in df.columns:
        df = df.rename(columns={df.columns[0]: "date"})

    rename_map = {}
    for c in df.columns:
        if c.replace(" ", "") == "mkt-rf" or c.replace(" ", "") == "mktrf":
            rename_map[c] = "mkt_rf"
        elif c == "smb":
            rename_map[c] = "smb"
        elif c == "hml":
            rename_map[c] = "hml"
        elif c == "rf":
            rename_map[c] = "rf"

    df = df.rename(columns=rename_map)

    required = ["date", "mkt_rf", "smb", "hml", "rf"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns in ff3: {missing}. got: {df.columns.tolist()}")

    df["asof_date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").dt.date
    df = df.drop(columns=["date"])

    for c in ["mkt_rf", "smb", "hml", "rf"]:
        df[c] = pd.to_numeric(df[c], errors="coerce") / 100.0

    df["source"] = "ken_french"
    df = df.dropna(subset=["asof_date"])
    df = df[["asof_date", "mkt_rf", "smb", "hml", "rf", "source"]]
    return df


def upsert_factors(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    eng = get_engine()
    sql = text("""
    insert into raw.raw_factors_ff3_daily
      (asof_date, mkt_rf, smb, hml, rf, source)
    values
      (:asof_date, :mkt_rf, :smb, :hml, :rf, :source)
    on conflict (asof_date)
    do update set
      mkt_rf = excluded.mkt_rf,
      smb = excluded.smb,
      hml = excluded.hml,
      rf = excluded.rf,
      source = excluded.source,
      ingested_at = now();
    """)
    rows = df.to_dict(orient="records")
    with eng.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def main() -> None:
    df = fetch_ff3_daily()
    inserted = upsert_factors(df)
    print(f"ff3 rows upserted: {inserted}")
    print(f"date range: {df['asof_date'].min()} to {df['asof_date'].max()}")


if __name__ == "__main__":
    main()
