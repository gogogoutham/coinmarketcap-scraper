CREATE TABLE IF NOT EXISTS currency (
    id SERIAL,
    name VARCHAR(255),
    symbol VARCHAR(10),
    slug VARCHAR(30),
    explorer_link TEXT,
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (id)
);
CREATE UNIQUE INDEX ON currency (slug);
CREATE INDEX ON currency (name);
CREATE INDEX ON currency (symbol);

CREATE TABLE IF NOT EXISTS currency_historical (
    name VARCHAR(255),
    symbol VARCHAR(10),
    slug VARCHAR(30),
    explorer_link TEXT,
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (name, symbol, slug, explorer_link)
);

CREATE TABLE IF NOT EXISTS market_cap_365 (
    currency INTEGER,
    time TIMESTAMP,
    market_cap_by_available_supply DECIMAL,
    market_cap_by_total_supply DECIMAL,
    price_usd DECIMAL,
    price_btc DECIMAL,
    est_available_supply DECIMAL,
    est_total_supply DECIMAL,
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY(currency, time)
);

CREATE UNIQUE INDEX ON market_cap_365 (time, currency);

CREATE TABLE IF NOT EXISTS market_cap_180 (
    currency INTEGER,
    time TIMESTAMP,
    market_cap_by_available_supply DECIMAL,
    market_cap_by_total_supply DECIMAL,
    price_usd DECIMAL,
    price_btc DECIMAL,
    est_available_supply DECIMAL,
    est_total_supply DECIMAL,
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY(currency, time)
);

CREATE UNIQUE INDEX ON market_cap_180 (time, currency);

CREATE TABLE IF NOT EXISTS market_cap_90 (
    currency INTEGER,
    time TIMESTAMP,
    market_cap_by_available_supply DECIMAL,
    market_cap_by_total_supply DECIMAL,
    price_usd DECIMAL,
    price_btc DECIMAL,
    est_available_supply DECIMAL,
    est_total_supply DECIMAL,
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY(currency, time)
);

CREATE UNIQUE INDEX ON market_cap_90 (time, currency);

CREATE TABLE IF NOT EXISTS market_cap_30 (
    currency INTEGER,
    time TIMESTAMP,
    market_cap_by_available_supply DECIMAL,
    market_cap_by_total_supply DECIMAL,
    price_usd DECIMAL,
    price_btc DECIMAL,
    est_available_supply DECIMAL,
    est_total_supply DECIMAL,
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY(currency, time)
);

CREATE UNIQUE INDEX ON market_cap_30 (time, currency);

CREATE TABLE IF NOT EXISTS market_cap_7 (
    currency INTEGER,
    time TIMESTAMP,
    market_cap_by_available_supply DECIMAL,
    market_cap_by_total_supply DECIMAL,
    price_usd DECIMAL,
    price_btc DECIMAL,
    est_available_supply DECIMAL,
    est_total_supply DECIMAL,
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY(currency, time)
);

CREATE UNIQUE INDEX ON market_cap_7 (time, currency);

CREATE TABLE IF NOT EXISTS trade_volume_usd (
    currency INTEGER,
    time TIMESTAMP,
    volume DECIMAL,
    PRIMARY KEY(currency, time)
);

CREATE UNIQUE INDEX ON trade_volume_usd (time, currency);