"""Synthetic banking data generator with intentional error injection."""
import csv
import os
import random
# from datetime import datetime, timedelta
from datetime import datetime, timedelta, timezone
from pathlib import Path
from faker import Faker

fake = Faker()
# OUTPUT_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data")) / "raw"

# src/generators/synthetic_banking.py → repo root is two levels up
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
OUTPUT_DIR = RAW_DIR

N_CUSTOMERS, N_ACCOUNTS, N_TRANSACTIONS = 500, 750, 10_000

ERROR_RATE_MISSING_CUSTOMER = 0.005
ERROR_RATE_NEGATIVE_AMOUNT  = 0.010
ERROR_RATE_DUPLICATE_TXN_ID = 0.005
ERROR_RATE_ORPHAN_ACCOUNT   = 0.010

CURRENCIES     = ["EUR", "USD", "GBP"]
ACCOUNT_TYPES  = ["checking", "savings", "credit"]
TXN_STATUSES   = ["success", "failed", "pending"]
MERCHANTS      = ["Amazon","Netflix","Spotify","Steam","IKEA","Tesco","Rimi","Bolt","Wolt","Airbnb"]


def _rand_dt(days_back: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )

def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"[generator] Wrote {len(rows)} rows to {path}")


def generate_customers() -> list[dict]:
    return [{
        "customer_id": f"C{i:06d}",
        "full_name": fake.name(),
        "email": fake.email(),
        "country": random.choice(["EE","LV","LT","FI","SE"]),
        "signup_date": _rand_dt(730).date().isoformat(),
    } for i in range(N_CUSTOMERS)]


def generate_accounts(customers: list[dict]) -> list[dict]:
    cids = [c["customer_id"] for c in customers]
    rows = []
    for i in range(N_ACCOUNTS):
        cid = "" if random.random() < ERROR_RATE_MISSING_CUSTOMER else random.choice(cids)
        rows.append({
            "account_id": f"A{i:06d}",
            "customer_id": cid,
            "account_type": random.choice(ACCOUNT_TYPES),
            "currency": random.choice(CURRENCIES),
            "opened_date": _rand_dt(730).date().isoformat(),
        })
    return rows


def generate_transactions(accounts: list[dict]) -> list[dict]:
    aids = [a["account_id"] for a in accounts]
    rows = []
    for i in range(N_TRANSACTIONS):
        # duplicate transaction_id sometimes
        if rows and random.random() < ERROR_RATE_DUPLICATE_TXN_ID:
            txn_id = rows[-1]["transaction_id"]
        else:
            txn_id = f"T{i:08d}"

        acc = "A999999" if random.random() < ERROR_RATE_ORPHAN_ACCOUNT else random.choice(aids)

        amount = round(random.uniform(1.0, 500.0), 2)
        if random.random() < ERROR_RATE_NEGATIVE_AMOUNT:
            amount = -amount

        rows.append({
            "transaction_id": txn_id,
            "account_id": acc,
            "transaction_date": _rand_dt(30).isoformat(),
            "amount": f"{amount}",
            "currency": random.choice(CURRENCIES),
            "status": random.choices(TXN_STATUSES, weights=[0.92, 0.06, 0.02])[0],
            "merchant": random.choice(MERCHANTS),
        })
    return rows


def generate_all() -> dict:
    customers    = generate_customers()
    accounts     = generate_accounts(customers)
    transactions = generate_transactions(accounts)

    _write_csv(OUTPUT_DIR / "customers.csv",
               ["customer_id","full_name","email","country","signup_date"], customers)
    _write_csv(OUTPUT_DIR / "accounts.csv",
               ["account_id","customer_id","account_type","currency","opened_date"], accounts)
    _write_csv(OUTPUT_DIR / "transactions.csv",
               ["transaction_id","account_id","transaction_date","amount","currency","status","merchant"], transactions)

    return {"customers": len(customers), "accounts": len(accounts), "transactions": len(transactions)}


if __name__ == "__main__":
    print(f"[generator] Summary: {generate_all()}")
