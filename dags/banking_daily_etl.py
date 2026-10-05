"""
Banking Data Reliability Platform — Version 4 DAG

Pipeline:
    start_run -> generate -> load_raw -> validate_transform_load
              -> quarantine_rejects -> dbt_run -> dbt_test
              -> quality_check -> reconciliation -> schema_drift
              -> compute_metrics -> publish_snapshot -> finish_run
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator

project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.generators.synthetic_banking import generate_all          # noqa: E402
from src.utils import reliability as rel                             # noqa: E402

#DATA_DIR = Path("/opt/airflow/data")
#DBT_DIR = Path("/opt/airflow/dbt")
#DBT_PROFILES_DIR = "/opt/airflow/dbt"
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DBT_DIR = PROJECT_ROOT / "dbt"
DBT_PROFILES_DIR = PROJECT_ROOT / "dbt"
DATA_DIR = PROJECT_ROOT / "data"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"



@dag(
    dag_id="banking_daily_etl",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    tags=["banking", "etl", "v4"],
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2),
                  "retry_exponential_backoff": True},
)
def banking_daily_etl():

    # ----------------------------------------------------------------
    # Monitoring — start
    # ----------------------------------------------------------------
    @task
    def start_run(**kwargs) -> str:
        dag_run_id = kwargs["run_id"]
        rel.start_pipeline_run(dag_run_id, "banking_daily_etl")
        return dag_run_id

    # ----------------------------------------------------------------
    # Core pipeline
    # ----------------------------------------------------------------
    @task
    def generate_synthetic_data() -> dict:
        summary = generate_all()
        print(f"[generate] {summary}")
        return summary

    @task
    def load_raw(summary: dict) -> dict:
        pg = PostgresHook(postgres_conn_id="postgres_banking")
        with pg.get_conn() as conn, conn.cursor() as cur:
            for t in ("customers", "accounts", "transactions"):
                cur.execute(f"TRUNCATE TABLE raw.{t};")

            def _copy(name, table, cols):
                p = DATA_DIR / "raw" / name
                with p.open("r", encoding="utf-8") as f:
                    cur.copy_expert(
                        f"COPY raw.{table} ({', '.join(cols)}) FROM STDIN WITH CSV HEADER",
                        f,
                    )
                print(f"[load_raw] Loaded {name} → raw.{table}")

            _copy("customers.csv", "customers",
                  ["customer_id", "full_name", "email", "country", "signup_date"])
            _copy("accounts.csv", "accounts",
                  ["account_id", "customer_id", "account_type", "currency", "opened_date"])
            _copy("transactions.csv", "transactions",
                  ["transaction_id", "account_id", "transaction_date", "amount",
                   "currency", "status", "merchant"])
        return summary

    @task
    def validate_transform_load(summary: dict) -> dict:
        pg = PostgresHook(postgres_conn_id="postgres_banking")
        with pg.get_conn() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE staging.transactions CASCADE;")
            cur.execute("TRUNCATE staging.accounts CASCADE;")
            cur.execute("TRUNCATE staging.customers CASCADE;")

            cur.execute("""
                INSERT INTO staging.customers
                    (customer_id, full_name, email, country, signup_date)
                SELECT customer_id, full_name, email, country, signup_date::date
                FROM raw.customers
                WHERE customer_id IS NOT NULL AND customer_id <> ''
                  AND full_name IS NOT NULL
                ON CONFLICT (customer_id) DO NOTHING;
            """)
            cur.execute("""
                INSERT INTO staging.accounts
                    (account_id, customer_id, account_type, currency, opened_date)
                SELECT a.account_id, a.customer_id, a.account_type, a.currency,
                       a.opened_date::date
                FROM raw.accounts a
                JOIN staging.customers c ON c.customer_id = a.customer_id
                WHERE a.account_id IS NOT NULL
                ON CONFLICT (account_id) DO NOTHING;
            """)
            cur.execute("""
                WITH candidates AS (
                    SELECT t.transaction_id, t.account_id,
                           t.transaction_date::timestamptz AS transaction_date,
                           t.amount::numeric AS amount,
                           t.currency, t.status, t.merchant,
                           ROW_NUMBER() OVER (
                               PARTITION BY t.transaction_id ORDER BY t.ingested_at
                           ) AS rn
                    FROM raw.transactions t
                    JOIN staging.accounts a ON a.account_id = t.account_id
                    WHERE t.transaction_id IS NOT NULL
                      AND t.amount::numeric > 0
                )
                INSERT INTO staging.transactions
                    (transaction_id, account_id, transaction_date, amount,
                     currency, status, merchant)
                SELECT transaction_id, account_id, transaction_date, amount,
                       currency, status, merchant
                FROM candidates WHERE rn = 1
                ON CONFLICT (transaction_id) DO NOTHING;
            """)
        return summary

    @task
    def quarantine_rejects(summary: dict) -> int:
        pg = PostgresHook(postgres_conn_id="postgres_banking")
        with pg.get_conn() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE quarantine.transactions;")

            cur.execute("""
                INSERT INTO quarantine.transactions
                    (transaction_id, account_id, transaction_date, amount,
                     currency, status, merchant, reject_reason)
                SELECT transaction_id, account_id, transaction_date, amount,
                       currency, status, merchant, 'negative_amount'
                FROM raw.transactions
                WHERE amount::numeric <= 0;
            """)
            cur.execute("""
                INSERT INTO quarantine.transactions
                    (transaction_id, account_id, transaction_date, amount,
                     currency, status, merchant, reject_reason)
                SELECT t.transaction_id, t.account_id, t.transaction_date, t.amount,
                       t.currency, t.status, t.merchant, 'orphan_account'
                FROM raw.transactions t
                LEFT JOIN staging.accounts a ON a.account_id = t.account_id
                WHERE a.account_id IS NULL;
            """)
            cur.execute("""
                WITH dupes AS (
                    SELECT transaction_id, account_id, transaction_date, amount,
                           currency, status, merchant,
                           ROW_NUMBER() OVER (
                               PARTITION BY transaction_id ORDER BY ingested_at
                           ) AS rn
                    FROM raw.transactions
                )
                INSERT INTO quarantine.transactions
                    (transaction_id, account_id, transaction_date, amount,
                     currency, status, merchant, reject_reason)
                SELECT transaction_id, account_id, transaction_date, amount,
                       currency, status, merchant, 'duplicate_transaction_id'
                FROM dupes WHERE rn > 1;
            """)
            cur.execute("SELECT COUNT(*) FROM quarantine.transactions;")
            count = cur.fetchone()[0]
        print(f"[quarantine_rejects] {count} rows quarantined")
        return count

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --no-version-check",
        retries=2,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --no-version-check",
        retries=1,
    )

    # ----------------------------------------------------------------
    # Quality check — records individual checks to ops.quality_checks
    # ----------------------------------------------------------------
    @task
    def quality_check(quarantine_count: int, **kwargs) -> dict:
        dag_run_id = kwargs["run_id"]
        pg = PostgresHook(postgres_conn_id="postgres_banking")
        with pg.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.transactions;")
            raw_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM analytics.fact_transactions;")
            fact_count = cur.fetchone()[0]

            reject_rate = quarantine_count / raw_count if raw_count else 1.0

            checks = [
                ("fact_table_populated",      fact_count > 0,     fact_count,     ">0"),
                ("reject_rate_below_10pct",   reject_rate < 0.10, f"{reject_rate:.4f}", "<0.10"),
                ("raw_to_fact_reasonable",    fact_count >= raw_count * 0.90,
                                              f"{fact_count}/{raw_count}", "≥90%"),
            ]

            failed = []
            for name, ok, obs, exp in checks:
                rel.record_quality_check(dag_run_id, name, ok, obs, exp)
                if not ok:
                    failed.append(name)

        if failed:
            raise ValueError(f"Quality checks failed: {failed}")
        print(f"[quality_check] All {len(checks)} checks passed (reject rate {reject_rate:.2%})")
        return {"fact_count": fact_count, "raw_count": raw_count}

    # ----------------------------------------------------------------
    # Reconciliation
    # ----------------------------------------------------------------
    @task
    def reconciliation(_: dict, **kwargs) -> None:
        dag_run_id = kwargs["run_id"]
        results = rel.reconcile_totals(dag_run_id)
        failed = [r for r in results if r["status"] == "failed"]
        if failed:
            raise ValueError(f"Reconciliation failed for: {[r['metric'] for r in failed]}")
        print(f"[reconciliation] All {len(results)} metrics reconciled")

    # ----------------------------------------------------------------
    # Schema drift detection
    # ----------------------------------------------------------------
    @task
    def schema_drift(**kwargs) -> None:
        dag_run_id = kwargs["run_id"]
        for table in ("customers", "accounts", "transactions"):
            cols = rel.capture_schema_snapshot(dag_run_id, table)
            result = rel.detect_schema_drift(dag_run_id, table, cols)
            if result["drift"]:
                raise ValueError(
                    f"Schema drift detected on raw.{table}: "
                    f"added={result['added']} removed={result['removed']}"
                )

    # ----------------------------------------------------------------
    # Business metrics
    # ----------------------------------------------------------------
    @task
    def compute_metrics(_: None) -> int:
        pg = PostgresHook(postgres_conn_id="postgres_banking")
        with pg.get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO analytics.daily_transaction_metrics (
                    metric_date, total_transactions, total_value,
                    successful_count, failed_count, success_rate,
                    avg_transaction, active_accounts, updated_at
                )
                SELECT transaction_date, COUNT(*), SUM(amount),
                       COUNT(*) FILTER (WHERE status = 'success'),
                       COUNT(*) FILTER (WHERE status = 'failed'),
                       ROUND(COUNT(*) FILTER (WHERE status = 'success')::numeric
                             / NULLIF(COUNT(*), 0), 4),
                       ROUND(AVG(amount), 2),
                       COUNT(DISTINCT account_id), NOW()
                FROM analytics.fact_transactions
                GROUP BY transaction_date
                ON CONFLICT (metric_date) DO UPDATE SET
                    total_transactions = EXCLUDED.total_transactions,
                    total_value        = EXCLUDED.total_value,
                    successful_count   = EXCLUDED.successful_count,
                    failed_count       = EXCLUDED.failed_count,
                    success_rate       = EXCLUDED.success_rate,
                    avg_transaction    = EXCLUDED.avg_transaction,
                    active_accounts    = EXCLUDED.active_accounts,
                    updated_at         = NOW();
            """)
            rows = cur.rowcount
        print(f"[compute_metrics] Wrote {rows} daily metric rows")
        return rows

    # ----------------------------------------------------------------
    # Publish CSV snapshots for the Streamlit dashboard
    # ----------------------------------------------------------------
    @task
    def publish_snapshot(_: int) -> dict:
        """Write CSV snapshots that Streamlit Cloud can read."""
        import csv

        # snapshot_dir = Path("/opt/airflow/dashboard/data/synthetic")
        snapshot_dir = DASHBOARD_DIR / "data" / "synthetic"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        pg = PostgresHook(postgres_conn_id="postgres_banking")

        # 1. Fact transactions — the main analytical table
        rows = pg.get_records("""
            SELECT transaction_id, account_id, customer_id, customer_country,
                   account_type, transaction_date, amount, currency, status, merchant
            FROM analytics.fact_transactions
            ORDER BY transaction_date DESC
            LIMIT 20000;
        """)
        fact_path = snapshot_dir / "fact_transactions.csv"
        with fact_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["transaction_id", "account_id", "customer_id",
                        "customer_country", "account_type", "transaction_date",
                        "amount", "currency", "status", "merchant"])
            w.writerows(rows)

        # 2. Daily metrics
        metrics = pg.get_records("""
            SELECT * FROM analytics.daily_transaction_metrics
            ORDER BY metric_date DESC;
        """)
        metrics_path = snapshot_dir / "daily_metrics.csv"
        with metrics_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["metric_date", "total_transactions", "total_value",
                        "successful_count", "failed_count", "success_rate",
                        "avg_transaction", "active_accounts", "updated_at"])
            w.writerows(metrics)

        # 3. Quarantine breakdown
        quarantine = pg.get_records("""
            SELECT reject_reason, COUNT(*) AS count
            FROM quarantine.transactions
            GROUP BY reject_reason ORDER BY 2 DESC;
        """)
        q_path = snapshot_dir / "quarantine.csv"
        with q_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["reject_reason", "count"])
            w.writerows(quarantine)

        # 4. Pipeline runs
        runs = pg.get_records("""
            SELECT run_id, dag_id, started_at, finished_at, status,
                   records_extracted, records_loaded, records_quarantined,
                   duration_seconds
            FROM ops.pipeline_runs
            ORDER BY started_at DESC LIMIT 30;
        """)
        runs_path = snapshot_dir / "pipeline_runs.csv"
        with runs_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["run_id", "dag_id", "started_at", "finished_at", "status",
                        "records_extracted", "records_loaded",
                        "records_quarantined", "duration_seconds"])
            w.writerows(runs)

        result = {
            "fact_rows": len(rows),
            "metric_rows": len(metrics),
            "quarantine_rows": len(quarantine),
            "run_rows": len(runs),
        }
        print(f"[publish_snapshot] {result}")
        return result

    # ----------------------------------------------------------------
    # Monitoring — finish (runs even if upstream failed)
    # ----------------------------------------------------------------
    @task(trigger_rule="all_done")
    def finish_run(generate_summary: dict, quarantine_count: int, **kwargs) -> None:
        dag_run_id = kwargs["run_id"]

        # In Airflow 3, the DAG run object is provided via the task context
        dag_run = kwargs.get("dag_run")
        if dag_run is not None and dag_run.state == "failed":
            status = "failed"
        else:
            status = "success"

        rel.finish_pipeline_run(
            dag_run_id=dag_run_id,
            status=status,
            records_extracted=(generate_summary or {}).get("transactions", 0),
            records_loaded=0,
            records_quarantined=quarantine_count or 0,
        )

    # ----------------------------------------------------------------
    # Wiring
    # ----------------------------------------------------------------
    # 1. Monitoring starts first
    run_id = start_run()

    # 2. Core ingestion chain (implicit deps via function args)
    summary     = generate_synthetic_data()
    loaded_raw  = load_raw(summary)
    staged      = validate_transform_load(loaded_raw)
    quarantined = quarantine_rejects(staged)

    # 3. Make generation wait for start_run
    run_id >> summary

    # 4. dbt chain: quarantine -> dbt_run -> dbt_test
    quarantined >> dbt_run >> dbt_test

    # 5. Quality + reconciliation chain (after dbt_test)
    qc_result = quality_check(quarantined)
    recon     = reconciliation(qc_result)
    drift     = schema_drift()
    metrics   = compute_metrics(None)
    finish    = finish_run(summary, quarantined)
    snapshot  = publish_snapshot(metrics)

    dbt_test >> qc_result >> recon >> drift >> metrics

    # 6. Finish writes final status to ops.pipeline_runs,
    #    THEN publish_snapshot reads it back and writes the CSV.
    metrics >> finish >> snapshot

banking_daily_etl()
