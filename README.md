# Banking Data Reliability Platform

> An Airflow-orchestrated data platform that ingests synthetic banking data, validates it, quarantines bad records, models it for analytics, and exposes KPIs and pipeline-health metrics.

## Project status

This project is being built in three maturity levels:

- [ ] **Version 1** — Basic reliable ETL pipeline (Airflow + Python + PostgreSQL)
- [ ] **Version 2** — Trusted data & analytical layer (dbt + data quality + BI)
- [ ] **Version 3** — Production-style reliability (monitoring + reconciliation + alerts)

## Tech stack (planned)

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 3.1 (LocalExecutor, Docker) |
| Language | Python 3.12 |
| Database | PostgreSQL 16 |
| Data generation | Faker |
| Transformations | pandas (V1), dbt (V2+) |
| Testing | pytest |
| CI/CD | GitHub Actions |
| Deployment | Docker Compose |

## Running locally

```bash
# 1. Clone
git clone https://github.com/AG-geodata-analyst/banking-data-reliability-platform.git
cd banking-data-reliability-platform

# 2. Set your user ID
echo "AIRFLOW_UID=$(id -u)" > .env

# 3. Build and initialize
docker compose up airflow-init

# 4. Start services
docker compose up -d

# 5. Open Airflow
# http://localhost:8081  (airflow / airflow)
