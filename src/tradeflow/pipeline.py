"""Command-line entry point that runs the pipeline end to end or stage by stage.

    python -m tradeflow.pipeline --stage all
    python -m tradeflow.pipeline --stage silver

Stages: ingest -> bronze -> silver -> gold -> ml. On Databricks the same
functions are called from the notebooks in ``notebooks/`` and chained as a
Lakeflow Job (see ``databricks.yml``).
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone

from tradeflow.config import PipelineConfig, load_config

log = logging.getLogger("tradeflow")
STAGES = ["ingest", "bronze", "silver", "gold", "ml"]


def run_ingest(cfg: PipelineConfig) -> dict:
    ing = cfg.ingestion
    landing = cfg.landing_path
    if ing["mode"] == "census_api":
        from tradeflow.ingestion.census_api import ingest_from_census

        rows = ingest_from_census(landing, ing["start_period"], ing["end_period"], ing["flows"],
                                  ing["commodity_level"])
        return {"stage": "ingest", "mode": "census_api", "rows_landed": rows}

    from tradeflow.ingestion.sample_generator import generate_sample

    stats = generate_sample(landing, cfg.path("data/reference"), ing["start_period"], ing["end_period"])
    return {"stage": "ingest", "mode": "sample", **stats}


def run_stage(stage: str, cfg: PipelineConfig, spark=None) -> dict:
    if stage == "ingest":
        return run_ingest(cfg)
    if stage == "bronze":
        from tradeflow.bronze import build_bronze
        return build_bronze(spark, cfg)
    if stage == "silver":
        from tradeflow.silver import build_silver
        return build_silver(spark, cfg)
    if stage == "gold":
        from tradeflow.gold import build_gold
        return build_gold(spark, cfg)
    if stage == "ml":
        from tradeflow.forecasting import train_and_forecast
        return train_and_forecast(spark, cfg)
    raise ValueError(f"unknown stage {stage}")


def log_run(spark, cfg: PipelineConfig, result: dict, started: datetime, seconds: float, status: str) -> None:
    from tradeflow.io import write_table

    row = [(result.get("stage"), status, started, round(seconds, 2), json.dumps(result, default=str))]
    df = spark.createDataFrame(row, "stage string, status string, started_at timestamp, "
                                    "duration_s double, metrics_json string")
    write_table(df, cfg, "ops", "pipeline_runs", mode="append", comment="Pipeline run log.")


def main(argv: list[str] | None = None) -> list[dict]:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stage", choices=STAGES + ["all"], default="all")
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_config(args.config)
    stages = STAGES if args.stage == "all" else [args.stage]

    spark = None
    if any(s != "ingest" for s in stages):
        from tradeflow.io import ensure_databases
        from tradeflow.spark_session import get_spark

        spark = get_spark(cfg)
        ensure_databases(spark, cfg)

    results = []
    for stage in stages:
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        t0 = time.perf_counter()
        result = run_stage(stage, cfg, spark)
        elapsed = time.perf_counter() - t0
        log.info("%s finished in %.1fs: %s", stage, elapsed, json.dumps(result, default=str))
        if spark is not None:
            log_run(spark, cfg, result, started, elapsed, "SUCCESS")
        results.append(result)
    return results


if __name__ == "__main__":
    main()
