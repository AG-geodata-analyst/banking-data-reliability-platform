"""
Reliability helpers: pipeline monitoring, reconciliation, schema drift.
Used by the banking_daily_etl DAG.
"""
from datetime import datetime, timezone
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook


def start_pipeline_run(dag_run_id: str, dag_id: str) -> int:
    """Insert a 'running' row into ops.pipeline_runs. Returns run_id."""
    pg = PostgresHook(postgres_conn_id="postgres_banking")
    with pg.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ops.pipeline_runs
                (dag_run_id, dag_id, started_at, status)
            VALUES (%s, %s, %s, 'running')
            RETURNING run_id;
        """, (dag_run_id, dag_id, datetime.now(timezone.utc)))
        run_id = cur.fetchone()[0]
    print(f"[monitoring] Started pipeline run #{run_id} ({dag_run_id})")
    return run_id


def finish_pipeline_run(
    dag_run_id: str,
    status: str,
    records_extracted: int = 0,
    records_loaded: int = 0,
    records_quarantined: int = 0,
    error_message: str | None = None,
) -> None:
    """Update the pipeline run row with final status and metrics."""
    pg = PostgresHook(postgres_conn_id="postgres_banking")
    with pg.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE ops.pipeline_runs
            SET finished_at          = NOW(),
                status               = %s,
                records_extracted    = %s,
                records_loaded       = %s,
                records_quarantined  = %s,
                duration_seconds     = EXTRACT(EPOCH FROM (NOW() - started_at)),
                error_message        = %s
            WHERE dag_run_id = %s;
        """, (status, records_extracted, records_loaded, records_quarantined,
              error_message, dag_run_id))
    print(f"[monitoring] Finished pipeline run {dag_run_id} → {status}")


def record_quality_check(
    dag_run_id: str,
    check_name: str,
    passed: bool,
    observed: Any = None,
    expected: Any = None,
    message: str = "",
) -> None:
    """Persist one quality check result."""
    pg = PostgresHook(postgres_conn_id="postgres_banking")
    with pg.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ops.quality_checks
                (dag_run_id, check_name, check_status,
                 observed_value, expected_value, message)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (dag_run_id, check_name, "passed" if passed else "failed",
              str(observed), str(expected), message))


def reconcile_totals(dag_run_id: str) -> list[dict]:
    """
    Compare source-side totals (raw.transactions) against target-side totals
    (analytics.fact_transactions). Record results in ops.reconciliation.
    """
    pg = PostgresHook(postgres_conn_id="postgres_banking")
    results: list[dict] = []

    with pg.get_conn() as conn, conn.cursor() as cur:
        # Metric 1: transaction count
        cur.execute("SELECT COUNT(*) FROM raw.transactions;")
        src_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM analytics.fact_transactions;")
        tgt_count = cur.fetchone()[0]

        # Metric 2: total value (source filtered to positive amounts)
        cur.execute("SELECT COALESCE(SUM(amount::numeric), 0) FROM raw.transactions WHERE amount::numeric > 0;")
        src_value = float(cur.fetchone()[0])
        cur.execute("SELECT COALESCE(SUM(amount), 0) FROM analytics.fact_transactions;")
        tgt_value = float(cur.fetchone()[0])

        # Metric 3: successful transactions
        cur.execute("SELECT COUNT(*) FROM raw.transactions WHERE status = 'success';")
        src_success = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM analytics.fact_transactions WHERE status = 'success';")
        tgt_success = cur.fetchone()[0]

        metrics = [
            ("transaction_count", src_count, tgt_count),
            ("transaction_value", src_value, tgt_value),
            ("successful_count",  src_success, tgt_success),
        ]

        for name, src, tgt in metrics:
            diff = src - tgt
            variance = (diff / src * 100) if src else 0.0
            # Tolerance: 5% variance is "ok", 10% is "warning", above is "failed"
            abs_var = abs(variance)
            if abs_var < 5:
                status = "ok"
            elif abs_var < 10:
                status = "warning"
            else:
                status = "failed"

            cur.execute("""
                INSERT INTO ops.reconciliation
                    (dag_run_id, metric_name, source_value, target_value,
                     difference, variance_pct, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (dag_run_id, name, src, tgt, diff, variance, status))

            results.append({
                "metric": name, "source": src, "target": tgt,
                "difference": diff, "variance_pct": variance, "status": status,
            })
            print(f"[reconciliation] {name}: src={src} tgt={tgt} "
                  f"var={variance:+.2f}% → {status}")

    return results


def capture_schema_snapshot(dag_run_id: str, table: str) -> list[str]:
    """Capture column names for a table and persist the snapshot."""
    pg = PostgresHook(postgres_conn_id="postgres_banking")
    with pg.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'raw' AND table_name = %s
            ORDER BY ordinal_position;
        """, (table,))
        columns = [row[0] for row in cur.fetchall()]

        cur.execute("""
            INSERT INTO ops.schema_snapshots
                (dag_run_id, table_name, column_names)
            VALUES (%s, %s, %s);
        """, (dag_run_id, table, columns))

    print(f"[schema_drift] Snapshot for raw.{table}: {columns}")
    return columns


def detect_schema_drift(dag_run_id: str, table: str, current_cols: list[str]) -> dict:
    """
    Compare current columns against the previous snapshot.
    Returns {'drift': bool, 'added': [...], 'removed': [...]}
    """
    pg = PostgresHook(postgres_conn_id="postgres_banking")
    with pg.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT column_names FROM ops.schema_snapshots
            WHERE table_name = %s
            ORDER BY captured_at DESC
            OFFSET 1 LIMIT 1;
        """, (table,))
        row = cur.fetchone()

    if row is None:
        # First-ever snapshot: no drift possible
        return {"drift": False, "added": [], "removed": [], "first_run": True}

    previous = set(row[0])
    current = set(current_cols)

    added = sorted(current - previous)
    removed = sorted(previous - current)
    drift = bool(added or removed)

    if drift:
        print(f"[schema_drift] DRIFT on raw.{table}: +{added} -{removed}")
    else:
        print(f"[schema_drift] No drift on raw.{table}")

    return {"drift": drift, "added": added, "removed": removed, "first_run": False}
