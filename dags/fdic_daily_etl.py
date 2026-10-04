"""
FDIC BankFind daily ETL.

Fetches live FDIC-insured bank data and publishes it as CSV snapshots
for the Streamlit dashboard.

Pipeline:
    start_run -> extract_fdic -> validate_fdic -> load_raw
             -> transform -> publish_snapshot -> quality_check -> finish_run
"""
import csv
import json
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.extract.fdic import fetch_institutions, summarise_institutions  # noqa: E402
from src.utils import reliability as rel                                 # noqa: E402

DATA_DIR = Path("/opt/airflow/data")
SNAPSHOT_DIR = Path("/opt/airflow/dashboard/data/fdic")


@dag(
    dag_id="fdic_daily_etl",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 3 * * *",          # 03:00 UTC daily (after the synthetic one)
    catchup=False,
    tags=["banking", "fdic", "real-data", "v4"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
)
def fdic_daily_etl():

    @task
    def start_run(**kwargs) -> str:
        rel.start_pipeline_run(kwargs["run_id"], "fdic_daily_etl")
        return kwargs["run_id"]

    @task
    def extract_fdic() -> dict:
        records = fetch_institutions()
        summary = summarise_institutions(records)

        # Persist raw JSON so the next tasks can read it from disk
        raw_path = DATA_DIR / "fdic_raw.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps(records, indent=2))
        print(f"[extract] {summary}")
        return {"raw_path": str(raw_path), **summary}

    @task
    def validate_fdic(summary: dict) -> dict:
        """Basic validation on the raw records."""
        records = json.loads(Path(summary["raw_path"]).read_text())

        valid, rejected = [], []
        for r in records:
            # Required fields
            if not r.get("CERT") or not r.get("NAME"):
                rejected.append({**r, "_reason": "missing_cert_or_name"})
                continue
            # Positive assets
            if (r.get("ASSET") or 0) <= 0:
                rejected.append({**r, "_reason": "non_positive_assets"})
                continue
            valid.append(r)

        valid_path = DATA_DIR / "fdic_valid.json"
        valid_path.write_text(json.dumps(valid, indent=2))

        result = {
            "valid_path": str(valid_path),
            "valid_count": len(valid),
            "rejected_count": len(rejected),
        }
        print(f"[validate] {result}")
        return result

    @task
    def publish_snapshot(validated: dict, **kwargs) -> dict:
        """Write CSVs that Streamlit Cloud will read."""
        records = json.loads(Path(validated["valid_path"]).read_text())
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

        # Institutions CSV
        institutions_path = SNAPSHOT_DIR / "institutions.csv"
        if records:
            fieldnames = list(records[0].keys())
            with institutions_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)

        # Metadata JSON
        meta_path = SNAPSHOT_DIR / "meta.json"
        meta = {
            "generated_at": kwargs["logical_date"].isoformat(),
            "institution_count": validated["valid_count"],
            "rejected_count": validated["rejected_count"],
        }
        meta_path.write_text(json.dumps(meta, indent=2))

        print(f"[publish_snapshot] Wrote {len(records)} rows to {institutions_path}")
        return meta

    @task
    def quality_check(validated: dict) -> None:
        """Fail the DAG if too few banks were extracted."""
        checks = {
            "at_least_100_institutions": validated["valid_count"] >= 100,
            "reject_rate_below_10pct": (
                validated["rejected_count"]
                / max(validated["valid_count"] + validated["rejected_count"], 1)
            ) < 0.10,
        }
        failed = [name for name, ok in checks.items() if not ok]
        if failed:
            raise ValueError(f"FDIC quality checks failed: {failed}")
        print(f"[quality_check] All {len(checks)} checks passed")

    @task(trigger_rule="all_done")
    def finish_run(extract_summary: dict, validated: dict, **kwargs) -> None:
        status = "success"
        dag_run = kwargs.get("dag_run")
        if dag_run is not None and dag_run.state == "failed":
            status = "failed"

        rel.finish_pipeline_run(
            dag_run_id=kwargs["run_id"],
            status=status,
            records_extracted=extract_summary.get("count", 0),
            records_loaded=validated.get("valid_count", 0),
            records_quarantined=validated.get("rejected_count", 0),
        )

    # Wiring
    run_id = start_run()
    extracted = extract_fdic()
    validated = validate_fdic(extracted)
    snapshot  = publish_snapshot(validated)
    qc        = quality_check(validated)

    run_id >> extracted >> validated
    validated >> snapshot
    validated >> qc
    finish_run(extracted, validated)


fdic_daily_etl()
