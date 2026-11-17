# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze — raw ingestion with Auto Loader
# MAGIC Incrementally loads new landing files into `trade_lakehouse.bronze.trade_flows_raw`
# MAGIC using Auto Loader (`cloudFiles`, `availableNow` trigger). All fields stay as strings;
# MAGIC lineage columns record the source file, its modification time, ingestion time and batch id.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from tradeflow.bronze import build_bronze  # noqa: E402
from tradeflow.io import ensure_databases  # noqa: E402

ensure_databases(spark, cfg)
print(build_bronze(spark, cfg))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT flow, COUNT(*) AS rows, COUNT(DISTINCT _source_file) AS files, MAX(_ingested_at) AS last_load
# MAGIC FROM trade_lakehouse.bronze.trade_flows_raw GROUP BY flow
