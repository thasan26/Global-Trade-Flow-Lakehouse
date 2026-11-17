# Databricks notebook source
# MAGIC %md
# MAGIC ### Shared setup
# MAGIC Puts the repo's `src/` folder on the Python path and loads the pipeline config.
# MAGIC Every other notebook runs this first with `%run ./_setup`.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
os.environ["TRADEFLOW_ROOT"] = REPO_ROOT
if os.path.join(REPO_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from tradeflow.config import load_config  # noqa: E402

cfg = load_config()
print("catalog:", cfg.catalog, "| landing:", cfg.landing_path, "| format:", cfg.table_format)
