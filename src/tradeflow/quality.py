"""Declarative data-quality rules (expectations) for the Silver layer.

Each rule is a SQL boolean expression that must be TRUE for a valid row.
A NULL result counts as a failure, so each rule tests one property and lets
the dedicated not-null rule own missing values. Severity decides what happens to rows
that fail:

* ``error`` - the row is routed to the quarantine table with the names of
  every rule it failed (like ``expect_or_drop`` in Lakeflow Declarative Pipelines).
* ``warn``  - the row is kept; the failure is only counted and logged.

Every run appends one result row per rule to ``ops.dq_results`` so data
quality can be tracked over time and shown on a dashboard.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)


@dataclass(frozen=True)
class Rule:
    name: str
    expression: str
    severity: str  # "error" | "warn"
    description: str


SILVER_RULES: list[Rule] = [
    Rule("value_not_null", "value_usd IS NOT NULL", "error",
         "Trade value must be present."),
    Rule("value_non_negative", "value_usd IS NULL OR value_usd >= 0", "error",
         "Trade values cannot be negative."),
    Rule("valid_period", "period_date IS NOT NULL AND period_date <= current_date()", "error",
         "Period must parse as YYYY-MM and not be in the future."),
    Rule("valid_hs2_code", "commodity_code RLIKE '^[0-9]{2}$'", "error",
         "Commodity code must be a two-digit HS chapter."),
    Rule("known_partner_country", "is_known_country", "error",
         "Partner code must exist in the country reference table."),
    Rule("known_hs_chapter", "is_known_chapter", "error",
         "HS chapter must exist in the commodity reference table."),
    Rule("value_plausible_range", "value_usd IS NULL OR value_usd < 100000000000", "warn",
         "Flag single partner/chapter months above $100B for review."),
    Rule("country_name_matches_reference", "cty_name_clean = upper(ref_country_name)", "warn",
         "Source country name should match the reference name after cleaning."),
]

DQ_RESULT_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("run_ts", TimestampType()),
    StructField("table_name", StringType()),
    StructField("rule_name", StringType()),
    StructField("severity", StringType()),
    StructField("description", StringType()),
    StructField("rows_checked", LongType()),
    StructField("rows_failed", LongType()),
    StructField("pass_rate", DoubleType()),
])


def apply_rules(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add ``_dq_errors`` and ``_dq_warnings`` array columns listing failed rules."""

    def failures(severity: str):
        checks = [
            F.when(~F.coalesce(F.expr(r.expression), F.lit(False)), F.lit(r.name))
            for r in rules if r.severity == severity
        ]
        if not checks:
            return F.array().cast("array<string>")
        return F.filter(F.array(*checks), lambda x: x.isNotNull())

    return df.withColumn("_dq_errors", failures("error")).withColumn("_dq_warnings", failures("warn"))


def split_valid_quarantine(checked: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = checked.where(F.size("_dq_errors") == 0)
    quarantine = checked.where(F.size("_dq_errors") > 0)
    return valid, quarantine


def summarize(spark: SparkSession, checked: DataFrame, rules: list[Rule], table_name: str, run_id: str) -> DataFrame:
    """One row per rule with failed-row counts, computed in a single pass."""
    aggs = [F.count(F.lit(1)).alias("__total")]
    for r in rules:
        col = "_dq_errors" if r.severity == "error" else "_dq_warnings"
        aggs.append(F.sum(F.array_contains(col, r.name).cast("long")).alias(r.name))
    row = checked.agg(*aggs).collect()[0]
    total = int(row["__total"])
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    data = [
        (run_id, now, table_name, r.name, r.severity, r.description, total,
         int(row[r.name] or 0), round(1 - (int(row[r.name] or 0) / total), 6) if total else 1.0)
        for r in rules
    ]
    return spark.createDataFrame(data, DQ_RESULT_SCHEMA)
