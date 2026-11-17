"""Client for the U.S. Census Bureau International Trade API.

Endpoints (HS-based, monthly):
    https://api.census.gov/data/timeseries/intltrade/exports/hs
    https://api.census.gov/data/timeseries/intltrade/imports/hs

The API returns a JSON array of arrays whose first row is the header. This
module requests one month and one flow per call, normalises the response to
the shared landing layout and writes it as NDJSON.

An API key is optional (set the ``CENSUS_API_KEY`` environment variable for a
higher rate limit). Docs: https://www.census.gov/data/developers/data-sets/international-trade.html
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import requests

from tradeflow.ingestion import LANDING_FIELDS

log = logging.getLogger(__name__)

BASE_URL = "https://api.census.gov/data/timeseries/intltrade/{flow}/hs"

# Variable names differ between the exports and imports endpoints.
FLOW_VARIABLES = {
    "exports": {
        "commodity": "E_COMMODITY",
        "description": "E_COMMODITY_LDESC",
        "value": "ALL_VAL_MO",       # total exports value, monthly (USD)
    },
    "imports": {
        "commodity": "I_COMMODITY",
        "description": "I_COMMODITY_LDESC",
        "value": "GEN_VAL_MO",       # general imports value, monthly (USD)
    },
}


class CensusTradeClient:
    def __init__(self, api_key: str | None = None, max_retries: int = 4, timeout: int = 60):
        self.api_key = api_key or os.environ.get("CENSUS_API_KEY")
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()

    def fetch_month(self, flow: str, period: str, comm_level: str = "HS2") -> list[dict[str, str]]:
        """Return all partner x commodity rows for one flow and month."""
        v = FLOW_VARIABLES[flow]
        params = {
            "get": f"CTY_CODE,CTY_NAME,{v['commodity']},{v['description']},{v['value']}",
            "time": period,
            "COMM_LVL": comm_level,
        }
        if self.api_key:
            params["key"] = self.api_key

        payload = self._get_with_retry(BASE_URL.format(flow=flow), params)
        if not payload:
            return []
        header, *rows = payload
        idx = {name: i for i, name in enumerate(header)}
        records = []
        for row in rows:
            records.append(
                {
                    "flow": flow,
                    "period": period,
                    "cty_code": row[idx["CTY_CODE"]],
                    "cty_name": row[idx["CTY_NAME"]],
                    "commodity_code": row[idx[v["commodity"]]],
                    "commodity_desc": row[idx[v["description"]]],
                    "value_usd": row[idx[v["value"]]],
                }
            )
        return records

    def _get_with_retry(self, url: str, params: dict) -> list | None:
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code == 204:          # no data published for this month yet
                    return None
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise requests.HTTPError(f"retryable status {resp.status_code}")
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as exc:
                if attempt == self.max_retries:
                    raise
                wait = 2 ** attempt
                log.warning("Census API attempt %s failed (%s); retrying in %ss", attempt, exc, wait)
                time.sleep(wait)
        return None


def month_range(start: str, end: str) -> list[str]:
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def write_landing(records: list[dict[str, str]], landing: Path, flow: str, period: str) -> Path:
    out_dir = landing / flow
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"period={period}.jsonl"
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps({k: rec.get(k) for k in LANDING_FIELDS}) + "\n")
    return path


def ingest_from_census(landing: Path, start: str, end: str, flows: list[str], comm_level: str = "HS2") -> int:
    """Pull every month in [start, end] for each flow into the landing zone.

    Months that already have a landing file are skipped, so re-runs only
    fetch new data (incremental ingestion).
    """
    client = CensusTradeClient()
    total = 0
    for flow in flows:
        for period in month_range(start, end):
            target = landing / flow / f"period={period}.jsonl"
            if target.exists():
                continue
            records = client.fetch_month(flow, period, comm_level)
            if records:
                write_landing(records, landing, flow, period)
                total += len(records)
                log.info("landed %s %s rows for %s", len(records), flow, period)
    return total
