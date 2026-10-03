CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ops;

-- RAW: untyped landing zone
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id TEXT, full_name TEXT, email TEXT, country TEXT, signup_date TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS raw.accounts (
    account_id TEXT, customer_id TEXT, account_type TEXT, currency TEXT, opened_date TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id TEXT, account_id TEXT, transaction_date TEXT, amount TEXT,
    currency TEXT, status TEXT, merchant TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- STAGING: typed, validated, deduplicated
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id TEXT PRIMARY KEY, full_name TEXT NOT NULL, email TEXT,
    country TEXT, signup_date DATE, ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS staging.accounts (
    account_id TEXT PRIMARY KEY,
    customer_id TEXT REFERENCES staging.customers(customer_id),
    account_type TEXT, currency TEXT, opened_date DATE,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT REFERENCES staging.accounts(account_id),
    transaction_date TIMESTAMPTZ NOT NULL,
    amount NUMERIC(18,2) NOT NULL, currency TEXT NOT NULL,
    status TEXT NOT NULL, merchant TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_stg_txn_date    ON staging.transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_stg_txn_account ON staging.transactions(account_id);

-- ANALYTICS: business-facing
CREATE TABLE IF NOT EXISTS analytics.daily_transaction_metrics (
    metric_date DATE PRIMARY KEY,
    total_transactions INTEGER NOT NULL,
    total_value NUMERIC(18,2) NOT NULL,
    successful_count INTEGER NOT NULL,
    failed_count INTEGER NOT NULL,
    success_rate NUMERIC(5,4),
    avg_transaction NUMERIC(18,2),
    active_accounts INTEGER,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- OPS: pipeline metadata (used in V2/V3)
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id SERIAL PRIMARY KEY, dag_run_id TEXT,
    started_at TIMESTAMPTZ NOT NULL, finished_at TIMESTAMPTZ,
    status TEXT, records_extracted INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0, records_rejected INTEGER DEFAULT 0
);
