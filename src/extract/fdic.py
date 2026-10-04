"""
FDIC BankFind API extractor.

Fetches institution-level data (name, location, assets, net income)
for FDIC-insured banks.

API docs: https://api.fdic.gov/banks/docs/
"""
import os
from datetime import datetime, timezone

import requests

FDIC_BASE_URL = "https://api.fdic.gov/banks"

# Fields we want. Must be UPPERCASE, comma-separated, no spaces.
INSTITUTION_FIELDS = [
    "CERT",       # FDIC certificate number (unique bank ID)
    "NAME",       # Legal name
    "CITY",       # HQ city
    "STALP",      # State abbreviation
    "STNUM",      # Street number
    "ASSET",      # Total assets (thousands of USD)
    "DEP",        # Total deposits
    "NETINC",     # Net income
    "ROA",        # Return on assets
    "ROE",        # Return on equity
    "ACTIVE",     # 1 = active, 0 = inactive
    "BKCLASS",    # Charter class (N, SM, NM, SB, SA, OI)
    "SPECGRP",    # Specialty group
    "REPDTE",     # Report date
]

PAGE_SIZE = 1000
MAX_PAGES = 10   # cap at 10,000 institutions to keep the CSV small


def fetch_institutions(api_key: str | None = None) -> list[dict]:
    """
    Fetch a page of FDIC-insured institutions.

    Returns a list of dicts, one per institution.
    """
    api_key = api_key or os.environ.get("FDIC_API_KEY")
    if not api_key:
        raise ValueError(
            "FDIC_API_KEY is not set. Register at https://api.fdic.gov/banks/docs/"
        )

    all_records: list[dict] = []

    for page in range(MAX_PAGES):
        params = {
            "filters": "ACTIVE:1",           # only active banks
            "fields":  ",".join(INSTITUTION_FIELDS),
            "sort_by": "ASSET",
            "descending": "true",
            "limit": PAGE_SIZE,
            "offset": page * PAGE_SIZE,
            "format": "json",
            "api_key": api_key,
        }

        response = requests.get(f"{FDIC_BASE_URL}/institutions", params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        # FDIC returns {"data": [{"data": {...}}, ...], "meta": {...}}
        rows = payload.get("data", [])
        if not rows:
            break

        all_records.extend(row["data"] for row in rows)
        print(f"[fdic] Page {page + 1}: fetched {len(rows)} institutions")

        # If we got less than a full page, we've reached the end
        if len(rows) < PAGE_SIZE:
            break

    print(f"[fdic] Total fetched: {len(all_records)} institutions")
    return all_records


def summarise_institutions(records: list[dict]) -> dict:
    """Quick metadata summary for logging."""
    return {
        "count": len(records),
        "states": len({r.get("STALP") for r in records if r.get("STALP")}),
        "total_assets_usd_billions": round(
            sum(r.get("ASSET") or 0 for r in records) / 1_000_000, 2
        ),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
