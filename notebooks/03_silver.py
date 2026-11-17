# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver — clean, deduplicate, validate, conform
# MAGIC * Casts and standardises fields, keeps only the latest revision of each record
# MAGIC * Joins country and HS-chapter reference data
# MAGIC * Applies declarative data-quality rules: failing rows go to `silver.trade_flows_quarantine`,
# MAGIC   results are appended to `ops.dq_results`, and the run fails if more than 2% of rows are quarantined.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from tradeflow.silver import build_silver  # noqa: E402

print(build_silver(spark, cfg))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rule_name, severity, rows_checked, rows_failed, ROUND(pass_rate * 100, 3) AS pass_pct
# MAGIC FROM trade_lakehouse.ops.dq_results
# MAGIC WHERE run_ts = (SELECT MAX(run_ts) FROM trade_lakehouse.ops.dq_results)
# MAGIC ORDER BY rows_failed DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT explode(_dq_errors) AS failed_rule, COUNT(*) AS rows
# MAGIC FROM trade_lakehouse.silver.trade_flows_quarantine GROUP BY 1 ORDER BY 2 DESC
