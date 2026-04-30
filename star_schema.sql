CREATE SCHEMA IF NOT EXISTS silver;

SET search_path TO silver;

CREATE TABLE IF NOT EXISTS silver.dim_accounts (
    account_id                  VARCHAR(40) PRIMARY KEY,
    first_name                  VARCHAR(50),
    last_name                   VARCHAR(50),
    email                       VARCHAR(100),
    phone                       VARCHAR(20),
    date_of_birth               DATE,
    currency                    VARCHAR(3),
    account_type                VARCHAR(50),
    account_sub_type            VARCHAR(50),
    acquisition_channel         VARCHAR(50),
    acquisition_channel_name    VARCHAR(100),
    initial_deposit             NUMERIC(15,4),
    registration_datetime       TIMESTAMP,
    churn_risk                  DECIMAL(5, 2),
    lifetime_value              DECIMAL(18, 2),
    updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.dim_date (
    date_id                     INT PRIMARY KEY,
    date                        DATE,
    day_name                    TEXT,
    month_name                  TEXT,
    month                       INT,
    quarter                     INT,
    year                        INT,
    is_weekend                  BOOLEAN
);

CREATE TABLE IF NOT EXISTS silver.dim_merchants (
    merchant_id                 SERIAL PRIMARY KEY,
    merchant_name               TEXT UNIQUE,
    category                    TEXT DEFAULT 'General'
);

CREATE TABLE IF NOT EXISTS silver.fact_transactions (
    transaction_id              VARCHAR(100) PRIMARY KEY,
    account_id                  VARCHAR(40) REFERENCES silver.dim_accounts(account_id),
    merchant_id                 INT REFERENCES silver.dim_merchants(merchant_id),
    date_id                     INT REFERENCES silver.dim_date(date_id),
    amount                      NUMERIC(15,4) NOT NULL,
    currency                    VARCHAR(3) NOT NULL,
    booking_datetime            TIMESTAMP NOT NULL,
    source                      VARCHAR(20),
    load_ts                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver.dim_date
SELECT 
    to_char(d, 'YYYYMMDD')::INT, d, to_char(d, 'TMDay'), to_char(d, 'TMMonth'),
    extract(month from d), extract(quarter from d), extract(year from d),
    CASE WHEN extract(isodow from d) IN (6, 7) THEN TRUE ELSE FALSE END
FROM generate_series('2025-01-01'::date, '2026-12-31'::date, '1 day'::interval) d
ON CONFLICT DO NOTHING;