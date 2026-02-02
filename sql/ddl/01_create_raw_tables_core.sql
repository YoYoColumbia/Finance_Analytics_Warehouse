create table if not exists raw.raw_prices (
  asof_date date not null,
  ticker text not null,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  volume numeric,
  source text not null default 'stooq',
  ingested_at timestamptz not null default now(),
  primary key (asof_date, ticker)
);

create table if not exists raw.raw_macro (
  asof_date date not null,
  series_id text not null,
  value numeric,
  source text not null default 'fred',
  ingested_at timestamptz not null default now(),
  primary key (asof_date, series_id)
);

create table if not exists raw.raw_trades (
  trade_id text primary key,
  trade_ts timestamptz not null,
  ticker text not null,
  side text not null,
  qty numeric not null,
  price numeric not null,
  fees numeric not null,
  portfolio_id text not null default 'p1',
  source text not null default 'simulated',
  ingested_at timestamptz not null default now()
);
