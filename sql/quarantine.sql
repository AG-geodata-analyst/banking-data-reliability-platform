CREATE SCHEMA IF NOT EXISTS quarantine;

CREATE TABLE IF NOT EXISTS quarantine.transactions (
    reject_id          SERIAL PRIMARY KEY,
    transaction_id     TEXT,
    account_id         TEXT,
    transaction_date   TEXT,
    amount             TEXT,
    currency           TEXT,
    status             TEXT,
    merchant           TEXT,
    reject_reason      TEXT NOT NULL,
    rejected_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_q_txn_reason ON quarantine.transactions(reject_reason);
