# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold — business aggregates
# MAGIC Builds the tables Power BI and the forecasting model read:
# MAGIC `trade_partner_monthly`, `trade_sector_monthly`, `partner_concentration`, `trade_kpis_annual`.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from tradeflow.gold import build_gold  # noqa: E402

print(build_gold(spark, cfg))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, flow, ROUND(total_usd / 1e9, 1) AS total_bn, yoy_growth_pct, top_partner, top_partner_share_pct
# MAGIC FROM trade_lakehouse.gold.trade_kpis_annual ORDER BY flow, year

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE trade_lakehouse.gold.trade_partner_monthly ZORDER BY (cty_code);
# MAGIC OPTIMIZE trade_lakehouse.gold.trade_sector_monthly ZORDER BY (sector);
