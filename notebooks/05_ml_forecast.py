# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Forecasting with MLflow
# MAGIC Trains a global gradient-boosting model on all 50 partner × flow series, backtests it
# MAGIC against naive baselines on the most recent 12 months, logs everything to MLflow and
# MAGIC registers the model in Unity Catalog as `trade_lakehouse.ml.trade_forecaster`.
# MAGIC Forecasts are written to `gold.trade_forecast` for Power BI.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements-databricks.txt
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from tradeflow.forecasting import train_and_forecast  # noqa: E402

result = train_and_forecast(spark, cfg)
result

# COMMAND ----------

# Promote the new version with an alias so downstream jobs always load "@champion"
from mlflow import MlflowClient  # noqa: E402

client = MlflowClient(registry_uri="databricks-uc")
name = result["registered_model"]
latest = max(int(v.version) for v in client.search_model_versions(f"name='{name}'"))
client.set_registered_model_alias(name, "champion", latest)
print(f"{name} v{latest} -> @champion")
