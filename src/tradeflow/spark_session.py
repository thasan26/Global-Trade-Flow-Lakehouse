"""SparkSession factory that works on Databricks, in CI and on a laptop."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from tradeflow.config import PROJECT_ROOT, PipelineConfig, is_databricks


def get_spark(cfg: PipelineConfig, app_name: str = "trade-flow-lakehouse") -> SparkSession:
    if is_databricks():
        # Databricks provides a pre-configured session (Unity Catalog + Delta).
        return SparkSession.builder.getOrCreate()

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    warehouse = PROJECT_ROOT / "spark-warehouse"
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "3g")
        # Persist table definitions between local runs (Databricks uses Unity Catalog instead)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={PROJECT_ROOT / '.metastore'}")
        .enableHiveSupport()
    )

    if cfg.table_format == "delta":
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        builder = configure_spark_with_delta_pip(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
