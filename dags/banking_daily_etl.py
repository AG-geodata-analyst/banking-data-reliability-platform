"""
Banking Data Reliability Platform — Version 2 DAG

Pipeline:
    generate -> load_raw -> validate_transform_load -> quarantine_rejects
             -> dbt_run -> dbt_test -> quality_check -> compute_metrics
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

from src.generators.synthetic_banking import generate_all  # noqa: E402

DATA_DIR   = Path("/opt/airflow/data")
DBT_DIR    = Path("/opt/airflow/dbt")
DBT_PROFILES_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="banking_daily_etl",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    tags=["banking", "etl", "v2"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
def banking_daily_etl():

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
                        f"COPY raw.{table} ({', '.join(cols)}) FROM STDIN WITH CSV HEADER", f
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
        """Move rejected raw transactions into quarantine.transactions."""
        pg = PostgresHook(postgres_conn_id="postgres_banking")
        with pg.get_conn() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE quarantine.transactions;")

            # Negative amounts
            cur.execute("""
                INSERT INTO quarantine.transactions
                    (transaction_id, account_id, transaction_date, amount,
                     currency, status, merchant, reject_reason)
                SELECT transaction_id, account_id, transaction_date, amount,
                       currency, status, merchant, 'negative_amount'
                FROM raw.transactions
                WHERE amount::numeric <= 0;
            """)

            # Orphan account
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

            # Duplicates (keep the first, quarantine the rest)
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
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --no-version-check"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --no-version-check"
        ),
    )

    @task
    def quality_check(quarantine_count: int) -> None:
        """Fail if quality drops below acceptable thresholds."""
        pg = PostgresHook(postgres_conn_id="postgres_banking")
        with pg.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.transactions;")
            raw_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM analytics.fact_transactions;")
            fact_count = cur.fetchone()[0]

            reject_rate = quarantine_count / raw_count if raw_count else 1.0

            checks = {
                "fact_table_populated":    fact_count > 0,
                "reject_rate_below_10pct": reject_rate < 0.10,
                "no_null_customer_in_fact": (
                    cur.execute("""
                        SELECT COUNT(*) FROM analytics.fact_transactions
                        WHERE customer_id IS NULL;
                    """) or cur.fetchone()[0] == 0
                ),
            }

        failed = [k for k, ok in checks.items() if not ok]
        if failed:
            raise ValueError(f"Quality checks failed: {failed}")
        print(f"[quality_check] All checks passed (reject rate {reject_rate:.2%})")

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

    # Wire the graph
    summary     = generate_synthetic_data()
    loaded_raw  = load_raw(summary)
    staged      = validate_transform_load(loaded_raw)
    quarantined = quarantine_rejects(staged)

    staged >> quarantined >> dbt_run >> dbt_test
    dbt_test >> quality_check(quarantined) >> compute_metrics(None)


banking_daily_etl()
