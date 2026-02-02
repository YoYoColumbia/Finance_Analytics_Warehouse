create table if not exists raw.raw_factors_ff3_daily (
  asof_date date not null,
  mkt_rf numeric,
  smb numeric,
  hml numeric,
  rf numeric,
  source text not null default 'ken_french',
  ingested_at timestamptz not null default now(),
  primary key (asof_date)
);
