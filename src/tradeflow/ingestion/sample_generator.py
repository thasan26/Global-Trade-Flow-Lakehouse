"""Sample-data generator for demos, CI and offline development.

Produces monthly U.S. exports/imports by partner country and HS2 chapter in
exactly the landing layout the Census API client writes, so every downstream
step runs the same code path in both modes.

The values are SYNTHETIC. Partner sizes, growth paths and product mixes are
shaped to look like real U.S. trade (e.g. Mexico and Canada as the largest
partners, imports from Vietnam rising, a 2020 pandemic dip, an energy price
spike in 2022), but they are not official statistics. Use
``TRADEFLOW_MODE=census_api`` for real data.

To exercise the data-quality layer, a small share of records is deliberately
corrupted the way real feeds go wrong: missing values, negative values,
malformed commodity codes, unknown country codes, inconsistent name casing,
and revised re-sends of records that were already delivered.
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import numpy as np

from tradeflow.ingestion import LANDING_FIELDS
from tradeflow.ingestion.census_api import month_range

# code: (annual imports $B, annual exports $B, import growth/yr, export growth/yr) - 2019 baseline
PARTNER_PROFILES: dict[str, tuple[float, float, float, float]] = {
    "1220": (320, 290, 0.035, 0.030),   # Canada
    "2010": (360, 255, 0.060, 0.045),   # Mexico
    "5700": (450, 105, -0.050, 0.010),  # China
    "5880": (145, 75, 0.005, 0.010),    # Japan
    "5800": (78, 57, 0.065, 0.030),     # South Korea
    "5830": (55, 31, 0.120, 0.050),     # Taiwan
    "5520": (67, 11, 0.150, 0.040),     # Vietnam
    "5570": (40, 12, 0.060, 0.020),     # Malaysia
    "5490": (32, 10, 0.090, 0.020),     # Thailand
    "5590": (27, 31, 0.040, 0.030),     # Singapore
    "6021": (10, 26, 0.030, 0.020),     # Australia
    "5330": (58, 34, 0.080, 0.050),     # India
    "4280": (128, 60, 0.030, 0.020),    # Germany
    "4120": (63, 69, 0.010, 0.020),     # United Kingdom
    "4279": (58, 38, 0.020, 0.020),     # France
    "4759": (57, 24, 0.035, 0.020),     # Italy
    "4210": (26, 51, 0.020, 0.040),     # Netherlands
    "4190": (62, 11, 0.070, 0.030),     # Ireland
    "4231": (22, 34, 0.010, 0.020),     # Belgium
    "4700": (15, 13, 0.030, 0.030),     # Spain
    "4419": (37, 18, 0.050, 0.020),     # Switzerland
    "5081": (20, 14, 0.010, 0.010),     # Israel
    "5170": (13, 14, -0.020, 0.000),    # Saudi Arabia
    "3510": (31, 43, 0.040, 0.030),     # Brazil
    "3010": (14, 15, 0.020, 0.030),     # Colombia
}

BASE_IMPORT_MIX = {
    "84": 14, "85": 14, "87": 12, "27": 8, "30": 7, "90": 4, "39": 3, "29": 3, "71": 3,
    "94": 3, "61": 2, "62": 2, "95": 2, "73": 2, "72": 2, "44": 1.5, "02": 1, "12": 0.5,
    "10": 0.5, "88": 2,
}
BASE_EXPORT_MIX = {
    "84": 12, "85": 10, "87": 8, "27": 14, "88": 8, "30": 5, "90": 5, "39": 5, "29": 4,
    "71": 4, "12": 4, "10": 3, "02": 3, "73": 2, "72": 1.5, "44": 1.5, "94": 1, "61": 0.5,
    "62": 0.5, "95": 1,
}
# Partner specialisations (multipliers on the base mix)
IMPORT_TILTS: dict[str, dict[str, float]] = {
    "5700": {"85": 2.2, "84": 1.6, "95": 4.0, "94": 3.0, "61": 2.5, "62": 2.5, "27": 0.05, "88": 0.1},
    "1220": {"27": 4.5, "87": 1.6, "44": 4.0, "30": 0.3},
    "2010": {"87": 2.0, "85": 1.7, "84": 1.6, "27": 0.6, "10": 0.1},
    "5170": {"27": 12.0},
    "4190": {"30": 7.0, "29": 4.0, "87": 0.05},
    "4419": {"30": 4.5, "71": 4.0, "90": 2.0},
    "5520": {"85": 2.5, "61": 4.0, "62": 4.0, "94": 4.0, "27": 0.05},
    "5880": {"87": 2.5, "84": 1.8, "27": 0.05},
    "4280": {"87": 2.2, "84": 1.7, "30": 1.6, "27": 0.05},
    "5800": {"87": 2.2, "85": 2.0, "27": 0.3},
    "5830": {"85": 3.5, "84": 2.5, "27": 0.05},
    "5330": {"30": 3.0, "71": 3.0, "62": 2.0},
    "3510": {"27": 2.5, "72": 3.0, "44": 3.0},
    "3010": {"27": 5.0, "71": 3.0},
    "5081": {"71": 6.0, "90": 2.0},
}
EXPORT_TILTS: dict[str, dict[str, float]] = {
    "1220": {"87": 2.0, "27": 1.5},
    "2010": {"27": 2.0, "85": 1.5, "02": 2.0, "10": 2.5},
    "5700": {"12": 6.0, "88": 1.5, "02": 2.0},
    "5880": {"27": 1.5, "30": 1.5},
    "4210": {"27": 2.5},
    "4120": {"71": 4.0, "88": 1.5},
    "4419": {"71": 6.0, "30": 2.0},
    "5590": {"27": 2.0, "88": 2.0},
}

SEASONALITY = np.array([0.95, 0.90, 1.02, 0.99, 1.01, 1.00, 0.98, 1.02, 1.00, 1.04, 1.03, 1.06])
COVID_SHOCK = {"2020-03": 0.88, "2020-04": 0.72, "2020-05": 0.74, "2020-06": 0.86, "2020-07": 0.93}


def _load_descriptions(reference_dir: Path) -> dict[str, str]:
    with open(reference_dir / "dim_hs_chapter.csv", encoding="utf-8") as fh:
        return {r["hs_chapter"]: r["chapter_description"].upper() for r in csv.DictReader(fh)}


def _load_country_names(reference_dir: Path) -> dict[str, str]:
    with open(reference_dir / "dim_country.csv", encoding="utf-8") as fh:
        return {r["cty_code"]: r["country_name"].upper() for r in csv.DictReader(fh)}


def _mix(base: dict[str, float], tilt: dict[str, float]) -> dict[str, float]:
    raw = {k: v * tilt.get(k, 1.0) for k, v in base.items()}
    total = sum(raw.values())
    return {k: v / total for k, v in raw.items()}


def _period_factor(period: str, chapter: str) -> float:
    year, month = map(int, period.split("-"))
    f = SEASONALITY[month - 1] * COVID_SHOCK.get(period, 1.0)
    if chapter in {"61", "62", "94", "95"} and month in (9, 10, 11):
        f *= 1.10                                   # holiday-season stocking
    if chapter == "27":                             # energy price cycle
        f *= {2020: 0.70, 2021: 0.95, 2022: 1.45, 2023: 1.10}.get(year, 1.0)
    if 2021 <= year <= 2022:
        f *= 1.06                                   # post-pandemic demand surge
    return f


def generate_sample(landing: Path, reference_dir: Path, start: str, end: str, seed: int = 42) -> dict[str, int]:
    rng = np.random.default_rng(seed)
    descriptions = _load_descriptions(reference_dir)
    names = _load_country_names(reference_dir)
    periods = month_range(start, end)
    stats = {"rows": 0, "injected_nulls": 0, "injected_negatives": 0, "injected_bad_codes": 0,
             "injected_unknown_country": 0, "revisions": 0}

    # Persistent per-series random walk so series have realistic momentum
    walk: dict[tuple[str, str, str], float] = {}

    for flow in ("exports", "imports"):
        out_dir = landing / flow
        out_dir.mkdir(parents=True, exist_ok=True)
        for t, period in enumerate(periods):
            records, revisions = [], []
            years_elapsed = t / 12
            for code, (imp, exp, g_imp, g_exp) in PARTNER_PROFILES.items():
                annual = imp if flow == "imports" else exp
                growth = g_imp if flow == "imports" else g_exp
                mix = _mix(BASE_IMPORT_MIX if flow == "imports" else BASE_EXPORT_MIX,
                           (IMPORT_TILTS if flow == "imports" else EXPORT_TILTS).get(code, {}))
                for chapter, share in mix.items():
                    key = (flow, code, chapter)
                    walk[key] = 0.85 * walk.get(key, 0.0) + rng.normal(0, 0.035)
                    value = (annual * 1e9 / 12) * share * (1 + growth) ** years_elapsed
                    value *= _period_factor(period, chapter) * np.exp(walk[key] + rng.normal(0, 0.05))
                    value_str = str(int(round(value)))
                    cty_name = names[code]
                    commodity = chapter

                    r = rng.random()
                    if r < 0.0015:
                        value_str = ""
                        stats["injected_nulls"] += 1
                    elif r < 0.0023:
                        value_str = "-" + value_str
                        stats["injected_negatives"] += 1
                    elif r < 0.0028:
                        commodity = chapter[0] + "A"
                        stats["injected_bad_codes"] += 1
                    if rng.random() < 0.02:
                        cty_name = cty_name.title() + "  "     # inconsistent casing / padding

                    rec = {"flow": flow, "period": period, "cty_code": code, "cty_name": cty_name,
                           "commodity_code": commodity, "commodity_desc": descriptions[chapter],
                           "value_usd": value_str}
                    records.append(rec)

                    if rng.random() < 0.006 and value_str and not value_str.startswith("-"):
                        rev = dict(rec, value_usd=str(int(int(value_str) * rng.uniform(0.98, 1.02))))
                        revisions.append(rev)

            if rng.random() < 0.25:                     # an unmapped partner code appears occasionally
                records.append({"flow": flow, "period": period, "cty_code": "9999",
                                "cty_name": "UNIDENTIFIED", "commodity_code": "84",
                                "commodity_desc": descriptions["84"], "value_usd": "1250000"})
                stats["injected_unknown_country"] += 1

            path = out_dir / f"period={period}.jsonl"
            with open(path, "w", encoding="utf-8") as fh:
                for rec in records:
                    fh.write(json.dumps({k: rec[k] for k in LANDING_FIELDS}) + "\n")
            stats["rows"] += len(records)

            if revisions:
                # Revised figures arrive later in a separate file (newer mtime)
                rev_path = out_dir / f"period={period}_revision.jsonl"
                with open(rev_path, "w", encoding="utf-8") as fh:
                    for rec in revisions:
                        fh.write(json.dumps({k: rec[k] for k in LANDING_FIELDS}) + "\n")
                st = os.stat(path)
                os.utime(rev_path, (st.st_atime + 86400, st.st_mtime + 86400))
                stats["rows"] += len(revisions)
                stats["revisions"] += len(revisions)
    return stats
