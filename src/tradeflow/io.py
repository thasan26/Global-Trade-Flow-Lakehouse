"""Table read/write helpers shared by every layer."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from tradeflow.config import PipelineConfig


def ensure_databases(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Create the layer schemas if they do not exist.

    On Databricks the catalog itself is created by ``sql/00_unity_catalog_setup.sql``
    (catalog creation needs metastore-admin rights, so it is kept out of the job).
    """
    for layer in cfg.raw["schemas"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.database(layer)}")


def write_table(
    df: DataFrame,
    cfg: PipelineConfig,
    layer: str,
    name: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    comment: str | None = None,
) -> str:
    fqn = cfg.table(layer, name)
    writer = df.write.format(cfg.table_format).mode(mode)
    if cfg.table_format == "delta" and mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(fqn)
    if comment and cfg.table_format == "delta":
        safe = comment.replace("'", "''")
        df.sparkSession.sql(f"COMMENT ON TABLE {fqn} IS '{safe}'")
    return fqn


def read_table(spark: SparkSession, cfg: PipelineConfig, layer: str, name: str) -> DataFrame:
    return spark.table(cfg.table(layer, name))
