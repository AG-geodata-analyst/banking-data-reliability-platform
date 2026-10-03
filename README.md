# 🏦 Banking Data Reliability Platform

> An Airflow-orchestrated data platform that ingests synthetic banking data, validates it, quarantines bad records, models it for analytics, and exposes KPIs alongside pipeline-health metrics.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![Airflow 3.1](https://img.shields.io/badge/airflow-3.1-017cee.svg)](https://airflow.apache.org/)
[![PostgreSQL 16](https://img.shields.io/badge/postgres-16-336791.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED.svg)](https://docs.docker.com/compose/)

---

## 🎯 The Problem

A bank receives data every day from multiple systems: customers, accounts, cards, payments, transactions, merchants, branches. Simply *storing* the data is not the hard part.

The hard part is:

> **How does a bank automatically collect data from many sources, detect problems before they silently corrupt analytics, create reliable datasets, and publish trustworthy information for business decisions?**

This project demonstrates how to solve that problem, one maturity level at a time.

### A concrete example

Imagine one day 1,000,000 transactions arrive:

- 500 have missing customer IDs
- 100 are duplicates
- 25 refer to accounts that don't exist
- One source suddenly renamed a column

A naive pipeline loads everything and silently corrupts every downstream report.

A **reliable** pipeline detects those problems, prevents the bad rows from reaching the analytical layer, and reports what happened.

That is exactly what this platform does.

---

## 🗺️ Three Versions, Three Levels of Maturity

This project is not three separate projects — it's **one repository that evolves**.

### Version 1 — Basic Reliable ETL Pipeline *(current)*

**Question answered:** *Can I build an automated end-to-end data pipeline?*

- Synthetic banking data (customers, accounts, transactions) generated with Faker
- Airflow DAG orchestrates extract → validate → transform → load
- PostgreSQL stores raw, staging, and analytics layers
- Basic data quality rules (positive amounts, referential integrity, unique IDs)
- Simple daily KPIs

### Version 2 — Trusted Data & Analytical Layer

**Question answered:** *Can I build a trustworthy analytical system?*

- dbt transformations with documented models
- Dimensional model: `fact_transactions` + `dim_customer`, `dim_account`, `dim_product`, `dim_date`
- Quarantine tables for rejected records
- Formal data quality checks (completeness, uniqueness, referential integrity, validity, freshness, consistency)
- Expanded business KPIs and BI dashboard

### Version 3 — Production-Style Reliability Platform

**Question answered:** *Can I build a reliable production-style platform?*

- Pipeline monitoring (`ops.pipeline_runs` table)
- Reconciliation between source totals and warehouse totals
- Schema-drift detection
- Retry handling with exponential backoff
- Idempotent processing (safe re-runs)
- Alerting on quality failures
- Operational "pipeline health" dashboard

---

## 🏗️ Target Architecture (end of Version 3)

```
Source systems (synthetic generator + public APIs)
        │
        ▼
Apache Airflow (orchestration)
        │
        ▼
Raw ingestion
        │
        ▼
Validation  ──►  Quarantine (bad records)
        │
        ▼
PostgreSQL — raw & staging schemas
        │
        ▼
dbt transformations
        │
        ▼
Analytical model (fact + dimensions)
        │
        ▼
Quality checks + Reconciliation
        │
        ▼
Business KPIs  +  Pipeline-health metrics
        │
        ▼
BI Dashboard (Streamlit)
        │
        ▼
GitHub Actions (daily automated runs)

Sidecar: logging · monitoring · retries · alerts
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 3.1.3 (LocalExecutor, Docker Compose) |
| Language | Python 3.12 |
| Database | PostgreSQL 16 |
| Data generation | Faker |
| Transformations | pandas (V1) → dbt-core (V2+) |
| Testing | pytest |
| Containerization | Docker Compose |
| CI/CD | GitHub Actions |
| Dashboard | Streamlit (V2+) |

---

## 📂 Project Structure

```
banking-data-reliability-platform/
├── dags/                          # Airflow DAGs
├── src/                           # Python source code
│   ├── __init__.py
│   ├── generators/                # Synthetic data generation (V1)
│   ├── validators/                # Data quality rules (V2+)
│   └── utils/                     # Shared helpers
├── sql/                           # SQL schemas and migrations
├── data/                          # Generated artifacts (gitignored)
│   ├── raw/
│   ├── processed/
│   └── quarantine/
├── docker/                        # Dockerfile
├── config/                        # Airflow config + auth
├── tests/                         # pytest tests
├── .github/workflows/             # CI/CD
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
├── LICENSE
└── README.md
```

---

## 🚀 Running Locally

### Prerequisites

- Docker Desktop (with WSL 2 backend if on Windows)
- WSL 2 (Ubuntu) — or any Linux/macOS environment
- ~4 GB free RAM

### 1. Clone and configure

```bash
git clone https://github.com/AG-geodata-analyst/banking-data-reliability-platform.git
cd banking-data-reliability-platform

# Set your WSL user ID (avoids file permission issues)
echo "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Build and initialize

```bash
docker compose build
docker compose up airflow-init
```

Wait for `airflow-init` to exit with code 0.

### 3. Start the stack

```bash
docker compose up -d
```

### 4. Open the Airflow UI

```
http://localhost:8081
```

Login with:

| Username | Password |
|---|---|
| `admin` | `admin` |

> If the login fails, check `config/simple_auth_manager_passwords.json.generated` — it should contain `{"admin": "admin"}`.

### 5. Verify the stack is healthy

```bash
docker compose ps
docker compose exec airflow-webserver curl -s http://localhost:8080/api/v2/monitor/health
```

All 5 services should be `(healthy)` or `Up`.

### 6. Stop everything

```bash
docker compose down          # stop (keeps data)
docker compose down -v       # stop + wipe database
```

---

## 📊 Data Model (Version 1)

### Schemas

| Schema | Purpose |
|---|---|
| `raw` | Data exactly as it arrived (all columns as TEXT) |
| `staging` | Cleaned, typed, validated data ready for analytics |
| `analytics` | Business-facing metrics and aggregated tables |
| `ops` | Pipeline metadata (run history, quality results) |

### Tables (planned)

| Table | Layer | Purpose |
|---|---|---|
| `raw.customers` | Raw | Untyped landing zone |
| `raw.accounts` | Raw | Untyped landing zone |
| `raw.transactions` | Raw | Untyped landing zone |
| `staging.customers` | Staging | Typed, deduplicated |
| `staging.accounts` | Staging | Typed, referentially validated |
| `staging.transactions` | Staging | Typed, positive amounts only, no orphans |
| `analytics.daily_transaction_metrics` | Analytics | KPIs per day |

---

## 🧪 Example Queries

Once the pipeline has run:

```sql
-- Row counts by layer
SELECT
    (SELECT COUNT(*) FROM raw.transactions)     AS raw_rows,
    (SELECT COUNT(*) FROM staging.transactions) AS staging_rows;

-- Daily KPIs
SELECT * FROM analytics.daily_transaction_metrics
ORDER BY metric_date DESC LIMIT 10;

-- Data-quality drop-off
SELECT
    ROUND(
        100.0 * (SELECT COUNT(*) FROM staging.transactions)
        / NULLIF((SELECT COUNT(*) FROM raw.transactions), 0),
        2
    ) AS staging_success_rate_pct;
```

---

## 🧠 Design Decisions

| Decision | Rationale |
|---|---|
| **Synthetic data (Faker) with injected errors** | Demonstrates validation and quarantine logic without touching real customer data (GDPR) |
| **Four-layer schema** (`raw` / `staging` / `analytics` / `ops`) | Mirrors a real data-warehouse design; each layer has a clear purpose |
| **Docker Compose** | Reproducible environment — anyone can run the project with one command |
| **LocalExecutor** | Simplest executor supporting parallel tasks; no Celery/Redis overhead for a portfolio project |
| **Custom ports** (8081, 5433) | Allows this project to coexist with other local Airflow/Postgres instances |
| **Airflow 3.1.3** | Latest stable major version; exercises modern features (api-server, TaskFlow) |
| **Versioned roadmap** | Each version is a concrete, portfolio-worthy milestone — not a vague future plan |

---

## 🛣️ Roadmap

- [x] **Scaffolding** — Docker Compose stack, ports, folder structure, README
- [ ] **Version 1** — Basic reliable ETL pipeline
- [ ] **Version 2** — dbt + data quality + dimensional model + BI
- [ ] **Version 3** — Monitoring, reconciliation, schema drift, alerting

---

## 🤝 Contributing

This is a personal portfolio project, but the structure is designed to be readable for anyone reviewing it. If you find a bug or have a suggestion, feel free to open an issue.

---

## 📜 License

Released under the [MIT License](LICENSE).

```
Copyright (c) 2026 Anderson Isaac Guamán Viveros
```

You are free to use, modify, and distribute this software, provided the original copyright notice and this permission notice are included in all copies or substantial portions of the software.

---

## 🙋 About

**Anderson Isaac Guamán Viveros**

Background in GIScience, Earth Observation, and Environmental Modelling.
Currently focused on data engineering, data quality, and analytics engineering.

- GitHub: [@AG-geodata-analyst](https://github.com/AG-geodata-analyst)
- Related project: [Estonia Environmental Forecast Monitor](https://github.com/AG-geodata-analyst/EE-environmental-monitor)

*Built with Apache Airflow, PostgreSQL, and Docker.*
