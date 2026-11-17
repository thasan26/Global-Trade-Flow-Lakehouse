"""Silver layer: typed, deduplicated, validated and conformed trade records.

Steps
1. Standardise: trim/uppercase text, cast value to a number, parse period.
2. Deduplicate: when Census revises a month, the revised record arrives in a
   newer file. Keep only the latest version of each
   (flow, period, country, commodity) key.
3. Conform: attach reference attributes (region, sector, ...) from the
   country and HS-chapter dimensions.
4. Validate: run the rules in ``quality.py``. Rows failing an error rule go to
   ``silver.trade_flows_quarantine`` with the reasons; the rest go to
   ``silver.trade_flows``. Rule results are appended to ``ops.dq_results``.
"""

from __future__ import annotations

import uuid

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from tradeflow.bronze import BRONZE_TABLE
from tradeflow.config import PipelineConfig
from tradeflow.io import read_table, write_table
from tradeflow.quality import SILVER_RULES, apply_rules, split_valid_quarantine, summarize

KEY = ["flow", "period", "cty_code", "commodity_code"]


class DataQualityError(RuntimeError):
    pass


def load_dimensions(spark: SparkSession, cfg: PipelineConfig) -> tuple[DataFrame, DataFrame]:
    # Read with pandas so this works the same on serverless compute, where
    # Spark cannot read workspace files directly.
    countries = pd.read_csv(cfg.path(cfg.raw["reference"]["countries"]), dtype=str)
    chapters = pd.read_csv(cfg.path(cfg.raw["reference"]["hs_chapters"]), dtype=str)
    return spark.createDataFrame(countries), spark.createDataFrame(chapters)


def standardize(bronze: DataFrame) -> DataFrame:
    return (
        bronze.select(
            F.lower(F.trim("flow")).alias("flow"),
            F.trim("period").alias("period"),
            F.trim("cty_code").alias("cty_code"),
            F.upper(F.regexp_replace(F.trim("cty_name"), r"\s+", " ")).alias("cty_name_clean"),
            F.lpad(F.trim("commodity_code"), 2, "0").alias("commodity_code"),
            F.trim("commodity_desc").alias("commodity_desc"),
            F.when(F.trim("value_usd") == "", None)
            .otherwise(F.trim("value_usd"))
            .cast("decimal(20,0)")
            .alias("value_usd"),
            "_source_file",
            "_file_modified_at",
            "_ingested_at",
            "_batch_id",
        )
        .withColumn("period_date", F.to_date(F.concat(F.col("period"), F.lit("-01")), "yyyy-MM-dd"))
    )


def deduplicate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Keep the newest version of each key. Returns (current, superseded)."""
    w = Window.partitionBy(*KEY).orderBy(F.col("_file_modified_at").desc(), F.col("_ingested_at").desc())
    ranked = df.withColumn("_version_rank", F.row_number().over(w))
    current = ranked.where("_version_rank = 1").drop("_version_rank")
    superseded = ranked.where("_version_rank > 1").drop("_version_rank")
    return current, superseded


def conform(df: DataFrame, dim_country: DataFrame, dim_chapter: DataFrame) -> DataFrame:
    c = dim_country.select(
        F.col("cty_code"), F.col("country_name").alias("ref_country_name"), "iso3", "region",
        "trade_agreement", F.lit(True).alias("is_known_country"),
    )
    h = dim_chapter.select(
        F.col("hs_chapter").alias("commodity_code"), "chapter_description", "sector",
        F.lit(True).alias("is_known_chapter"),
    )
    return (
        df.join(F.broadcast(c), "cty_code", "left")
        .join(F.broadcast(h), "commodity_code", "left")
        .fillna({"is_known_country": False, "is_known_chapter": False})
    )


def build_silver(spark: SparkSession, cfg: PipelineConfig) -> dict:
    run_id = str(uuid.uuid4())
    dim_country, dim_chapter = load_dimensions(spark, cfg)
    write_table(dim_country, cfg, "silver", "dim_country",
                comment="Partner-country reference: Census code, ISO3, region, trade agreement.")
    write_table(dim_chapter, cfg, "silver", "dim_hs_chapter",
                comment="HS2 chapter reference with sector grouping.")

    bronze = read_table(spark, cfg, "bronze", BRONZE_TABLE)
    current, superseded = deduplicate(standardize(bronze))
    checked = apply_rules(conform(current, dim_country, dim_chapter), SILVER_RULES).cache()
    valid, quarantine = split_valid_quarantine(checked)

    silver_cols = [
        "flow", "period", "period_date", "cty_code", F.col("ref_country_name").alias("country_name"),
        "iso3", "region", "trade_agreement", "commodity_code", "chapter_description", "sector",
        F.col("value_usd").cast("double").alias("value_usd"), "_dq_warnings",
        "_source_file", "_ingested_at", "_batch_id",
    ]
    write_table(valid.select(*silver_cols), cfg, "silver", "trade_flows", partition_by=["flow"],
                comment="Validated, deduplicated monthly U.S. trade by partner and HS2 chapter.")
    write_table(
        quarantine.select("flow", "period", "cty_code", "cty_name_clean", "commodity_code",
                          F.col("value_usd").cast("string").alias("value_usd"), "_dq_errors",
                          "_source_file", "_ingested_at", "_batch_id"),
        cfg, "silver", "trade_flows_quarantine",
        comment="Records that failed an error-level data-quality rule, with the failed rule names.",
    )

    dq = summarize(spark, checked, SILVER_RULES, cfg.table("silver", "trade_flows"), run_id)
    write_table(dq, cfg, "ops", "dq_results", mode="append",
                comment="Per-run data-quality rule results.")

    n_checked = checked.count()
    n_quarantined = quarantine.count()
    n_superseded = superseded.count()
    checked.unpersist()

    ratio = n_quarantined / n_checked if n_checked else 0.0
    if ratio > cfg.quality["max_quarantine_ratio"]:
        raise DataQualityError(
            f"{ratio:.2%} of rows quarantined (limit {cfg.quality['max_quarantine_ratio']:.2%}). "
            "Inspect silver.trade_flows_quarantine before publishing Gold."
        )
    return {
        "stage": "silver", "run_id": run_id, "rows_checked": n_checked,
        "rows_valid": n_checked - n_quarantined, "rows_quarantined": n_quarantined,
        "revisions_superseded": n_superseded, "quarantine_ratio": round(ratio, 5),
    }
