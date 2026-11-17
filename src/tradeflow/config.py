"""Pipeline configuration loading.

Configuration lives in ``conf/pipeline.yaml``. A few values can be overridden
with environment variables so the same code runs locally, in CI and on
Databricks without edits:

* ``TRADEFLOW_CATALOG``       - Unity Catalog name
* ``TRADEFLOW_TABLE_FORMAT``  - ``delta`` or ``parquet``
* ``TRADEFLOW_MODE``          - ``sample`` or ``census_api``
* ``TRADEFLOW_ROOT``          - project root (for resolving relative paths)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(os.environ.get("TRADEFLOW_ROOT", Path(__file__).resolve().parents[2]))


def is_databricks() -> bool:
    """True when running inside a Databricks runtime (classic or serverless)."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@dataclass
class PipelineConfig:
    raw: dict[str, Any]
    root: Path = field(default=PROJECT_ROOT)

    # ---- namespace -------------------------------------------------------
    @property
    def catalog(self) -> str:
        return os.environ.get("TRADEFLOW_CATALOG", self.raw["catalog"])

    def schema(self, layer: str) -> str:
        return self.raw["schemas"][layer]

    def table(self, layer: str, name: str) -> str:
        """Fully qualified table name.

        On Databricks this is the Unity Catalog three-level name
        ``catalog.schema.table``. Locally there is no Unity Catalog, so the
        schema is prefixed with the catalog name to keep names unique.
        """
        if is_databricks():
            return f"{self.catalog}.{self.schema(layer)}.{name}"
        return f"{self.catalog}_{self.schema(layer)}.{name}"

    def database(self, layer: str) -> str:
        if is_databricks():
            return f"{self.catalog}.{self.schema(layer)}"
        return f"{self.catalog}_{self.schema(layer)}"

    # ---- storage ---------------------------------------------------------
    @property
    def table_format(self) -> str:
        return os.environ.get("TRADEFLOW_TABLE_FORMAT", self.raw.get("table_format", "delta"))

    # ---- ingestion -------------------------------------------------------
    @property
    def ingestion(self) -> dict[str, Any]:
        cfg = dict(self.raw["ingestion"])
        cfg["mode"] = os.environ.get("TRADEFLOW_MODE", cfg["mode"])
        return cfg

    @property
    def landing_path(self) -> Path:
        """Where raw files land before Bronze ingestion.

        On Databricks this is a Unity Catalog Volume, so raw files are governed
        by the same permissions as the tables built from them.
        """
        if "TRADEFLOW_LANDING_PATH" in os.environ:
            return Path(os.environ["TRADEFLOW_LANDING_PATH"])
        if is_databricks():
            return Path(f"/Volumes/{self.catalog}/{self.schema('bronze')}/landing")
        return self.path(self.raw["ingestion"]["landing_path"])

    @property
    def quality(self) -> dict[str, Any]:
        return self.raw["quality"]

    @property
    def ml(self) -> dict[str, Any]:
        return self.raw["ml"]

    def path(self, relative: str) -> Path:
        p = Path(relative)
        return p if p.is_absolute() else self.root / p


def load_config(path: str | Path | None = None) -> PipelineConfig:
    cfg_path = Path(path) if path else PROJECT_ROOT / "conf" / "pipeline.yaml"
    with open(cfg_path, encoding="utf-8") as fh:
        return PipelineConfig(raw=yaml.safe_load(fh))
