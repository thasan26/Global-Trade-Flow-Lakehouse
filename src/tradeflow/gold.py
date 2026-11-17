"""Gold layer: business-ready aggregates for Power BI and the forecasting model.

Tables
* ``gold.trade_partner_monthly``  - exports, imports, balance and YoY growth per partner and month
* ``gold.trade_sector_monthly``   - value and share of total by sector / HS chapter
* ``gold.partner_concentration``  - Herfindahl-Hirschman Index (HHI) and top-5 share per month
* ``gold.trade_kpis_annual``      - yearly totals, growth and the top partner per flow
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from tradeflow.config import PipelineConfig
from tradeflow.io import read_table, write_table


def partner_monthly(silver: DataFrame) -> DataFrame:
    base = (
        silver.groupBy("period_date", "cty_code", "country_name", "iso3", "region", "trade_agreement")
        .pivot("flow", ["exports", "imports"])
        .agg(F.sum("value_usd"))
        .fillna(0, ["exports", "imports"])
        .withColumnRenamed("exports", "exports_usd")
        .withColumnRenamed("imports", "imports_usd")
        .withColumn("total_trade_usd", F.col("exports_usd") + F.col("imports_usd"))
        .withColumn("trade_balance_usd", F.col("exports_usd") - F.col("imports_usd"))
    )
    # Year-over-year via a self-join on (period - 12 months): robust to missing months.
    prior = base.select(
        F.add_months("period_date", 12).alias("period_date"), "cty_code",
        F.col("exports_usd").alias("exports_usd_py"), F.col("imports_usd").alias("imports_usd_py"),
    )
    yoy = lambda cur, prev: F.round((F.col(cur) - F.col(prev)) / F.col(prev) * 100, 2)  # noqa: E731
    return (
        base.join(prior, ["period_date", "cty_code"], "left")
        .withColumn("exports_yoy_pct", yoy("exports_usd", "exports_usd_py"))
        .withColumn("imports_yoy_pct", yoy("imports_usd", "imports_usd_py"))
        .withColumn("year", F.year("period_date"))
        .withColumn("month", F.month("period_date"))
        .drop("exports_usd_py", "imports_usd_py")
    )


def sector_monthly(silver: DataFrame) -> DataFrame:
    agg = silver.groupBy("period_date", "flow", "sector", "commodity_code", "chapter_description").agg(
        F.sum("value_usd").alias("value_usd")
    )
    w = Window.partitionBy("period_date", "flow")
    return agg.withColumn("share_of_flow_pct", F.round(F.col("value_usd") / F.sum("value_usd").over(w) * 100, 3))


def concentration(silver: DataFrame) -> DataFrame:
    by_partner = silver.groupBy("period_date", "flow", "cty_code").agg(F.sum("value_usd").alias("v"))
    w_tot = Window.partitionBy("period_date", "flow")
    w_rank = Window.partitionBy("period_date", "flow").orderBy(F.col("v").desc())
    shares = by_partner.withColumn("share", F.col("v") / F.sum("v").over(w_tot)).withColumn(
        "rank", F.row_number().over(w_rank)
    )
    return shares.groupBy("period_date", "flow").agg(
        F.round(F.sum(F.pow(F.col("share") * 100, 2)), 1).alias("hhi"),
        F.round(F.sum(F.when(F.col("rank") <= 5, F.col("share")).otherwise(0)) * 100, 2).alias("top5_share_pct"),
        F.countDistinct("cty_code").alias("partner_count"),
    )


def annual_kpis(silver: DataFrame) -> DataFrame:
    yearly = silver.withColumn("year", F.year("period_date"))
    totals = yearly.groupBy("year", "flow").agg(
        F.sum("value_usd").alias("total_usd"),
        F.countDistinct("period").alias("months_reported"),
    )
    w_flow = Window.partitionBy("flow").orderBy("year")
    totals = totals.withColumn(
        "yoy_growth_pct",
        F.round((F.col("total_usd") / F.lag("total_usd").over(w_flow) - 1) * 100, 2),
    )
    by_partner = yearly.groupBy("year", "flow", "country_name").agg(F.sum("value_usd").alias("v"))
    w_top = Window.partitionBy("year", "flow").orderBy(F.col("v").desc())
    top = by_partner.withColumn("r", F.row_number().over(w_top)).where("r = 1").select(
        "year", "flow", F.col("country_name").alias("top_partner"), F.col("v").alias("top_partner_usd")
    )
    return totals.join(top, ["year", "flow"]).withColumn(
        "top_partner_share_pct", F.round(F.col("top_partner_usd") / F.col("total_usd") * 100, 2)
    )


def build_gold(spark: SparkSession, cfg: PipelineConfig) -> dict:
    silver = read_table(spark, cfg, "silver", "trade_flows").cache()
    outputs = {
        "trade_partner_monthly": (partner_monthly(silver),
                                  "Monthly exports, imports, balance and YoY growth by partner country."),
        "trade_sector_monthly": (sector_monthly(silver),
                                 "Monthly trade value and share by sector and HS2 chapter."),
        "partner_concentration": (concentration(silver),
                                  "Monthly partner concentration: HHI and top-5 partner share."),
        "trade_kpis_annual": (annual_kpis(silver),
                              "Annual totals, growth and top partner by trade flow."),
    }
    counts = {}
    for name, (df, comment) in outputs.items():
        fqn = write_table(df, cfg, "gold", name, comment=comment)
        counts[name] = spark.table(fqn).count()
    silver.unpersist()
    return {"stage": "gold", **{f"rows_{k}": v for k, v in counts.items()}}
