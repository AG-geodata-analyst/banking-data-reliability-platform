CREATE SCHEMA IF NOT EXISTS ops;

-- ============================================================
-- Every DAG run records one row here
-- ============================================================
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id              SERIAL PRIMARY KEY,
    dag_run_id          TEXT NOT NULL,
    dag_id              TEXT NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ,
    status              TEXT,   -- running | success | failed
    records_extracted   INTEGER DEFAULT 0,
    records_loaded      INTEGER DEFAULT 0,
    records_quarantined INTEGER DEFAULT 0,
    duration_seconds    NUMERIC(10, 2),
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_started ON ops.pipeline_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status  ON ops.pipeline_runs(status);

-- ============================================================
-- One row per quality check per run
-- ============================================================
CREATE TABLE IF NOT EXISTS ops.quality_checks (
    check_id        SERIAL PRIMARY KEY,
    dag_run_id      TEXT NOT NULL,
    check_name      TEXT NOT NULL,
    check_status    TEXT NOT NULL,    -- passed | failed
    observed_value  TEXT,
    expected_value  TEXT,
    message         TEXT,
    checked_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qc_run ON ops.quality_checks(dag_run_id);

-- ============================================================
-- Reconciliation snapshots: source vs target totals
-- ============================================================
CREATE TABLE IF NOT EXISTS ops.reconciliation (
    recon_id         SERIAL PRIMARY KEY,
    dag_run_id       TEXT NOT NULL,
    metric_name      TEXT NOT NULL,       -- e.g. "transaction_count", "transaction_value"
    source_value     NUMERIC(20, 4),
    target_value     NUMERIC(20, 4),
    difference       NUMERIC(20, 4),
    variance_pct     NUMERIC(10, 6),
    status           TEXT NOT NULL,       -- ok | warning | failed
    checked_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_recon_run ON ops.reconciliation(dag_run_id);

-- ============================================================
-- Schema snapshots: source column names, captured per run
-- ============================================================
CREATE TABLE IF NOT EXISTS ops.schema_snapshots (
    snapshot_id      SERIAL PRIMARY KEY,
    dag_run_id       TEXT NOT NULL,
    table_name       TEXT NOT NULL,
    column_names     TEXT[] NOT NULL,
    captured_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schema_run ON ops.schema_snapshots(dag_run_id);
