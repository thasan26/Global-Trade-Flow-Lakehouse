"""Bronze layer: raw landing files -> append-only table with lineage columns.

Bronze keeps every field as a string exactly as received. Nothing is cleaned
or dropped here, so any record can be traced back to its source file and
re-processed if Silver logic changes.

* Locally: a batch read of the landing directory.
* On Databricks: Auto Loader (``cloudFiles``) with ``availableNow`` - it
  tracks which files were already ingested in a checkpoint, so each run only
  picks up new files (incremental, exactly-once).
"""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from tradeflow.config import PipelineConfig, is_databricks
from tradeflow.ingestion import LANDING_FIELDS
from tradeflow.io import write_table

RAW_SCHEMA = StructType([StructField(name, StringType(), True) for name in LANDING_FIELDS])
BRONZE_TABLE = "trade_flows_raw"


def _with_lineage(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def build_bronze(spark: SparkSession, cfg: PipelineConfig) -> dict:
    batch_id = str(uuid.uuid4())
    landing = str(cfg.landing_path)

    if is_databricks():
        checkpoint = f"/Volumes/{cfg.catalog}/{cfg.schema('ops')}/checkpoints/bronze_trade_flows"
        stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("recursiveFileLookup", "true")
            .schema(RAW_SCHEMA)
            .load(landing)
        )
        query = (
            _with_lineage(stream, batch_id)
            .writeStream.option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(cfg.table("bronze", BRONZE_TABLE))
        )
        query.awaitTermination()
        rows = spark.table(cfg.table("bronze", BRONZE_TABLE)).where(F.col("_batch_id") == batch_id).count()
        return {"stage": "bronze", "batch_id": batch_id, "rows_written": rows}

    raw = (
        spark.read.schema(RAW_SCHEMA)
        .option("recursiveFileLookup", "true")
        .json(landing)
    )
    bronze = _with_lineage(raw, batch_id)
    # Local runs rebuild Bronze from the full landing zone each time.
    fqn = write_table(
        bronze, cfg, "bronze", BRONZE_TABLE, mode="overwrite", partition_by=["flow"],
        comment="Raw U.S. trade records as received from the landing zone, with lineage columns.",
    )
    rows = spark.table(fqn).count()
    return {"stage": "bronze", "batch_id": batch_id, "rows_written": rows}
