# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingest to landing zone
# MAGIC Pulls monthly U.S. exports and imports by partner country and HS2 chapter into the
# MAGIC Unity Catalog volume `/Volumes/trade_lakehouse/bronze/landing`.
# MAGIC
# MAGIC * `census_api` mode calls the U.S. Census International Trade API (incremental: months already landed are skipped).
# MAGIC * `sample` mode writes the bundled sample dataset in the same format.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

dbutils.widgets.dropdown("mode", "census_api", ["census_api", "sample"], "Ingestion mode")
os.environ["TRADEFLOW_MODE"] = dbutils.widgets.get("mode")

from tradeflow.pipeline import run_ingest  # noqa: E402

result = run_ingest(load_config())
print(result)

# COMMAND ----------

display(dbutils.fs.ls(str(cfg.landing_path) + "/imports")[:10])
