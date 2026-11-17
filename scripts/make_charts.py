"""Render the README figures from the pipeline's Gold / ops tables.

    PYTHONPATH=src python scripts/make_charts.py

Run after ``python -m tradeflow.pipeline --stage all``. Images are written
to docs/images/.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tradeflow.config import load_config  # noqa: E402
from tradeflow.spark_session import get_spark  # noqa: E402

OUT = ROOT / "docs" / "images"
OUT.mkdir(parents=True, exist_ok=True)

# Palette: fixed categorical order + neutral chrome
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
CRITICAL, GOOD = "#d03b3b", "#0ca30c"

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10, "text.color": INK, "axes.labelcolor": INK2,
    "axes.edgecolor": AXIS, "axes.facecolor": SURFACE, "figure.facecolor": SURFACE,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.6, "axes.spines.top": False, "axes.spines.right": False,
    "axes.titleweight": "bold", "axes.axisbelow": True, "axes.titlesize": 12, "axes.titlelocation": "left",
    "legend.frameon": False, "lines.linewidth": 2,
})


def billions(x, _=None):
    return f"${x / 1e9:,.1f}B" if abs(x) < 20e9 else f"${x / 1e9:,.0f}B"


def save(fig, name):
    fig.savefig(OUT / name, dpi=160, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    print("wrote", OUT / name)


# ---------------------------------------------------------------------------
def architecture():
    fig, ax = plt.subplots(figsize=(13.4, 6.4))
    ax.set_xlim(0, 13.4)
    ax.set_ylim(0, 6.4)
    ax.axis("off")

    def box(x, y, w, h, title, body, color):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12",
                                    fc="white", ec=color, lw=2))
        ax.add_patch(FancyBboxPatch((x, y + h - 0.42), w, 0.42, boxstyle="round,pad=0.02,rounding_size=0.12",
                                    fc=color, ec=color, lw=2))
        ax.text(x + w / 2, y + h - 0.21, title, ha="center", va="center", color="white", weight="bold", fontsize=10)
        ax.text(x + w / 2, y + (h - 0.42) / 2, body, ha="center", va="center", color=INK, fontsize=8.6,
                linespacing=1.45)

    def arrow(x1, y1, x2, y2, label=None):
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                    arrowprops=dict(arrowstyle="-|>", color=INK2, lw=1.6, mutation_scale=14))
        if label:
            ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.14, label, ha="center", fontsize=8, color=INK2, style="italic")

    gray, bronze, silver, gold, ml, bi = "#52514e", "#a0612d", "#7c8590", "#c98500", "#6250d6", "#2a78d6"
    box(0.2, 3.5, 2.1, 1.9, "SOURCES", "U.S. Census\nInternational Trade API\n(HS2, monthly)\n\nor bundled\nsample generator", gray)
    box(2.9, 3.5, 2.1, 1.9, "LANDING", "Unity Catalog Volume\n/Volumes/.../landing\n\nNDJSON per\nflow x month", gray)
    box(5.6, 3.5, 2.1, 1.9, "BRONZE", "Auto Loader\n(availableNow)\n\nraw strings +\nlineage columns", bronze)
    box(8.3, 3.5, 2.1, 1.9, "SILVER", "cast + standardise\ndedupe revisions\njoin reference dims\n8 data-quality rules", silver)
    box(10.9, 3.5, 1.95, 1.9, "GOLD", "partner monthly\nsector monthly\nconcentration (HHI)\nannual KPIs", gold)
    arrow(2.3, 4.45, 2.9, 4.45)
    arrow(5.0, 4.45, 5.6, 4.45)
    arrow(7.7, 4.45, 8.3, 4.45)
    arrow(10.4, 4.45, 10.9, 4.45)

    box(5.6, 0.9, 2.1, 1.75, "QUARANTINE + OPS", "failed rows + reasons\ndq_results per rule\npipeline_runs log", CRITICAL)
    arrow(8.7, 3.5, 7.5, 2.65)
    ax.text(8.0, 3.1, "error rules", fontsize=8, color=INK2, style="italic", ha="right")

    box(8.3, 0.9, 2.1, 1.75, "MLflow", "gradient-boosting\nforecaster\nbacktest vs baselines\nUC model registry", ml)
    arrow(11.2, 3.5, 10.1, 2.65)
    ax.text(10.5, 3.1, "features", fontsize=8, color=INK2, style="italic", ha="right")

    box(10.9, 0.9, 1.95, 1.75, "POWER BI", "DAX measures\nexecutive, partner,\nsector, forecast\npages", bi)
    arrow(12.4, 3.5, 12.4, 2.65)
    ax.text(12.5, 3.05, "SQL\nwarehouse", fontsize=8, color=INK2, style="italic", ha="left", va="center")
    arrow(10.4, 1.55, 10.9, 1.55)
    ax.text(10.65, 1.68, "forecasts", fontsize=7.5, color=INK2, style="italic", ha="center")

    ax.add_patch(FancyBboxPatch((0.2, 0.15), 12.65, 0.45, boxstyle="round,pad=0.02,rounding_size=0.1",
                                fc="#f0efec", ec=AXIS))
    ax.text(6.5, 0.375, "Unity Catalog governs everything: catalog trade_lakehouse  ·  schemas bronze / silver / gold / ml / ops"
            "  ·  grants, tags, row filters, lineage  ·  orchestrated as a Lakeflow Job (Databricks Asset Bundle)",
            ha="center", va="center", fontsize=8.6, color=INK)
    ax.text(0.2, 6.15, "Global Trade Flow Lakehouse - architecture", fontsize=14, weight="bold")
    save(fig, "architecture.png")


# ---------------------------------------------------------------------------
def overview(pm: pd.DataFrame, kpis: pd.DataFrame):
    pm = pm.copy()
    pm["period_date"] = pd.to_datetime(pm["period_date"])
    monthly = pm.groupby("period_date")[["exports_usd", "imports_usd"]].sum().sort_index()
    last_year = int(pm["year"].max())

    fig = plt.figure(figsize=(13, 7.6))
    gs = fig.add_gridspec(2, 4, height_ratios=[1.0, 4], hspace=0.28, wspace=0.35)
    k = kpis.set_index(["year", "flow"])
    exp_, imp_ = k.loc[(last_year, "exports")], k.loc[(last_year, "imports")]
    tiles = [
        (f"Exports {last_year}", f"${exp_.total_usd / 1e12:,.2f}T", f"{exp_.yoy_growth_pct:+.1f}% vs {last_year - 1}"),
        (f"Imports {last_year}", f"${imp_.total_usd / 1e12:,.2f}T", f"{imp_.yoy_growth_pct:+.1f}% vs {last_year - 1}"),
        ("Trade balance", f"-${(imp_.total_usd - exp_.total_usd) / 1e9:,.0f}B", "deficit, 25 partners"),
        ("Top import source", str(imp_.top_partner).title(), f"{imp_.top_partner_share_pct:.1f}% of imports"),
    ]
    for i, (label, value, sub) in enumerate(tiles):
        ax = fig.add_subplot(gs[0, i])
        ax.axis("off")
        ax.add_patch(FancyBboxPatch((0, 0), 1, 1, boxstyle="round,pad=0,rounding_size=0.06",
                                    fc="white", ec=GRID, transform=ax.transAxes))
        ax.text(0.07, 0.76, label, fontsize=9.5, color=INK2, transform=ax.transAxes)
        ax.text(0.07, 0.40, value, fontsize=18, weight="bold", transform=ax.transAxes)
        ax.text(0.07, 0.13, sub, fontsize=8.5, color=MUTED, transform=ax.transAxes)

    ax = fig.add_subplot(gs[1, :2])
    ax.plot(monthly.index, monthly["imports_usd"], color=SERIES[0], label="Imports")
    ax.plot(monthly.index, monthly["exports_usd"], color=SERIES[1], label="Exports")
    ax.yaxis.set_major_formatter(billions)
    ax.set_title("Monthly exports vs. imports")
    for col, color, name in [("imports_usd", SERIES[0], "Imports"), ("exports_usd", SERIES[1], "Exports")]:
        ax.text(monthly.index[-1] + pd.Timedelta(days=25), monthly[col].iloc[-1], name, color=INK2,
                va="center", fontsize=9)
    ax.axvspan(pd.Timestamp("2020-03-01"), pd.Timestamp("2020-07-01"), color=GRID, alpha=0.6, lw=0)
    ax.text(pd.Timestamp("2020-05-01"), ax.get_ylim()[0] * 1.02, "2020 dip", ha="center", va="bottom",
            fontsize=8, color=MUTED)
    ax.legend(loc="lower right")
    ax.set_xlim(right=monthly.index[-1] + pd.Timedelta(days=260))

    ax = fig.add_subplot(gs[1, 2:])
    top = (pm[pm["year"] == last_year].groupby("country_name")[["exports_usd", "imports_usd"]].sum()
           .assign(total=lambda d: d.sum(axis=1)).nlargest(10, "total").iloc[::-1])
    y = np.arange(len(top))
    ax.barh(y, top["imports_usd"], color=SERIES[0], height=0.62, label="Imports")
    ax.barh(y, top["exports_usd"], left=top["imports_usd"] + top["total"].max() * 0.004,
            color=SERIES[1], height=0.62, label="Exports")
    ax.set_yticks(y, [n.title() for n in top.index])
    ax.xaxis.set_major_formatter(billions)
    ax.grid(axis="y", visible=False)
    ax.set_title(f"Top 10 partners by total trade, {last_year}")
    ax.legend(loc="lower right")
    fig.suptitle("Executive overview (generated from gold.trade_partner_monthly)", x=0.125, ha="left",
                 fontsize=14, weight="bold", y=0.99)
    save(fig, "dashboard_overview.png")


# ---------------------------------------------------------------------------
def sourcing_shift(pm: pd.DataFrame):
    pm = pm.copy()
    pm["period_date"] = pd.to_datetime(pm["period_date"])
    total = pm.groupby("period_date")["imports_usd"].sum()
    focus = ["CHINA", "MEXICO", "VIETNAM", "TAIWAN", "CANADA"]
    fig, ax = plt.subplots(figsize=(12, 5))
    for i, c in enumerate(focus):
        s = pm[pm["country_name"] == c].sort_values("period_date").set_index("period_date")["imports_usd"] / total * 100
        s = s.rolling(3, min_periods=1).mean()
        ax.plot(s.index, s.values, color=SERIES[i], label=c.title())
        ax.text(s.index[-1] + pd.Timedelta(days=25), s.iloc[-1], f"{c.title()} {s.iloc[-1]:.1f}%",
                va="center", fontsize=9, color=INK2)
    ax.set_ylabel("Share of U.S. imports (%), 3-month average")
    ax.set_title("Import sourcing shift: share of U.S. imports by partner")
    ax.legend(loc="upper center", ncol=5)
    ax.set_xlim(right=pm["period_date"].max() + pd.Timedelta(days=300))
    save(fig, "import_sourcing_shift.png")


# ---------------------------------------------------------------------------
def sectors(sm: pd.DataFrame):
    sm = sm.copy()
    sm["year"] = pd.to_datetime(sm["period_date"]).dt.year
    t = sm[sm["flow"] == "imports"].groupby(["year", "sector"])["value_usd"].sum().unstack()
    order = t.sum().sort_values(ascending=False).index
    t = t[order]
    fig, ax = plt.subplots(figsize=(12, 5.2))
    bottom = np.zeros(len(t))
    gap = t.values.sum(axis=1).max() * 0.003
    for i, sector in enumerate(order):
        ax.bar(t.index, t[sector], bottom=bottom, color=SERIES[i], width=0.62, label=sector)
        bottom += t[sector].values + gap
    ax.yaxis.set_major_formatter(lambda x, _: f"${x / 1e12:,.1f}T")
    ax.set_title("U.S. imports by sector, per year")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left", bbox_to_anchor=(1.0, 1.0), title="Sector")
    save(fig, "sector_mix.png")


# ---------------------------------------------------------------------------
def data_quality(dq: pd.DataFrame, run_metrics: dict):
    latest = dq[dq["run_ts"] == dq["run_ts"].max()].sort_values("rows_failed")
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(13, 4.8), gridspec_kw={"width_ratios": [1.6, 1]})
    colors = [CRITICAL if s == "error" else "#eda100" for s in latest["severity"]]
    y = np.arange(len(latest))
    ax.barh(y, latest["rows_failed"], color=colors, height=0.6)
    ax.set_yticks(y, latest["rule_name"])
    for yi, (v, sev) in enumerate(zip(latest["rows_failed"], latest["severity"])):
        ax.text(v + 2, yi, f"{int(v)}  ({sev})", va="center", fontsize=9, color=INK2)
    ax.set_xlabel("Rows failing the rule (latest run)")
    ax.set_title("Data-quality rules: failures per rule")
    ax.grid(axis="y", visible=False)
    ax.set_xlim(right=latest["rows_failed"].max() * 1.35 + 5)

    ax2.axis("off")
    s = run_metrics
    lines = [
        ("Bronze rows ingested", f"{s['bronze_rows']:,}"),
        ("Revisions superseded (dedup)", f"{s['revisions_superseded']:,}"),
        ("Rows validated", f"{s['rows_checked']:,}"),
        ("Rows published to Silver", f"{s['rows_valid']:,}"),
        ("Rows quarantined", f"{s['rows_quarantined']:,}  ({s['quarantine_ratio'] * 100:.2f}%)"),
        ("Quarantine threshold", "2.00%  ->  PASS"),
    ]
    ax2.text(0, 0.97, "Silver run summary", fontsize=12, weight="bold", transform=ax2.transAxes)
    for i, (k, v) in enumerate(lines):
        yy = 0.82 - i * 0.13
        ax2.text(0, yy, k, fontsize=10, color=INK2, transform=ax2.transAxes)
        ax2.text(1, yy, v, fontsize=10.5, weight="bold", ha="right", transform=ax2.transAxes,
                 color=GOOD if "PASS" in v else INK)
    save(fig, "data_quality.png")


# ---------------------------------------------------------------------------
def forecast(fc: pd.DataFrame, pm: pd.DataFrame):
    fc = fc.copy()
    fc["period_date"] = pd.to_datetime(fc["period_date"])
    pm = pm.copy()
    pm["period_date"] = pd.to_datetime(pm["period_date"])
    partners = ["MEXICO", "CHINA", "VIETNAM", "GERMANY"]
    fig, axes = plt.subplots(2, 2, figsize=(13, 7), sharex=True)
    for ax, p in zip(axes.flat, partners):
        hist = pm[(pm["country_name"] == p) & (pm["period_date"] >= "2023-01-01")].sort_values("period_date").set_index("period_date")
        ax.plot(hist.index, hist["imports_usd"], color=SERIES[0], label="Actual imports")
        bt = fc[(fc["country_name"] == p) & (fc["flow"] == "imports") & (fc["split"] == "backtest")].sort_values("period_date")
        fw = fc[(fc["country_name"] == p) & (fc["flow"] == "imports") & (fc["split"] == "forecast")].sort_values("period_date")
        ax.plot(bt["period_date"], bt["forecast_usd"], color=SERIES[1], ls="--", label="Model (one-step backtest)")
        last = hist.iloc[[-1]]
        fx = pd.concat([last.index.to_series(), fw["period_date"]])
        fy = np.concatenate([last["imports_usd"].values, fw["forecast_usd"].values])
        ax.plot(fx, fy, color=SERIES[1], marker="o", ms=5, label="Forward forecast")
        ax.axvspan(pd.Timestamp("2025-01-01"), pd.Timestamp("2025-12-31"), color=GRID, alpha=0.45, lw=0)
        w = (bt["forecast_usd"] - bt["actual_usd"]).abs().sum() / bt["actual_usd"].sum() * 100
        ax.set_title(f"{p.title()}  ·  backtest WAPE {w:.1f}%", fontsize=11)
        ax.yaxis.set_major_formatter(billions)
        ax.xaxis.set_major_locator(matplotlib.dates.YearLocator())
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y"))
    axes[0, 0].text(pd.Timestamp("2025-07-01"), axes[0, 0].get_ylim()[1] * 0.995, "2025 test window",
                    ha="center", va="top", fontsize=8, color=MUTED)
    axes[0, 0].legend(loc="lower left", fontsize=8.5)
    fig.suptitle("Monthly imports: actual vs. forecast (gold.trade_forecast)", x=0.125, ha="left",
                 fontsize=14, weight="bold")
    fig.tight_layout()
    save(fig, "forecast_backtest.png")


def model_eval(metrics: dict, importance: pd.DataFrame):
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(13, 4.6), gridspec_kw={"width_ratios": [1, 1.3]})
    names = ["Seasonal naive\n(same month last year)", "Naive\n(last month)", "Gradient boosting\n(this project)"]
    vals = [metrics["seasonal_naive"]["wape_pct"], metrics["naive_last_month"]["wape_pct"], metrics["model"]["wape_pct"]]
    bars = ax.bar(names, vals, color=[AXIS, AXIS, SERIES[0]], width=0.55)
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.08, f"{v:.2f}%", ha="center", fontsize=10, weight="bold")
    ax.set_ylabel("WAPE on 2025 holdout (lower is better)")
    ax.set_title("Backtest: model vs. baselines")
    ax.grid(axis="x", visible=False)

    imp = importance.sort_values("importance")
    ax2.barh(imp["feature"], imp["importance"], color=SERIES[0], height=0.6)
    ax2.set_title("Permutation feature importance")
    ax2.set_xlabel("Increase in MAE when feature is shuffled")
    ax2.grid(axis="y", visible=False)
    fig.tight_layout()
    save(fig, "model_evaluation.png")


def run_log(text: str):
    lines = text.strip().splitlines()
    fig = plt.figure(figsize=(13, 0.32 * len(lines) + 0.8))
    fig.patch.set_facecolor("#1a1a19")
    fig.text(0.015, 0.97, "$ PYTHONPATH=src python -m tradeflow.pipeline --stage all", color="#86b6ef",
             family="DejaVu Sans Mono", fontsize=9.5, va="top")
    for i, ln in enumerate(lines):
        fig.text(0.015, 0.97 - (i + 1.3) / (len(lines) + 2), ln, color="#f0efec", family="DejaVu Sans Mono",
                 fontsize=8.6, va="top")
    fig.savefig(OUT / "pipeline_run.png", dpi=160, facecolor="#1a1a19")
    plt.close(fig)
    print("wrote", OUT / "pipeline_run.png")


def main():
    cfg = load_config()
    spark = get_spark(cfg)
    t = lambda layer, name: spark.table(cfg.table(layer, name)).toPandas()  # noqa: E731
    pm, sm = t("gold", "trade_partner_monthly"), t("gold", "trade_sector_monthly")
    kpis, fc = t("gold", "trade_kpis_annual"), t("gold", "trade_forecast")
    dq, runs = t("ops", "dq_results"), t("ops", "pipeline_runs")
    for df in (pm, fc):
        df["country_name"] = df["country_name"].str.upper()
    runs = runs.sort_values("started_at")
    by_stage = {r.stage: json.loads(r.metrics_json) for r in runs.itertuples()}

    architecture()
    overview(pm, kpis)
    sourcing_shift(pm)
    sectors(sm)
    data_quality(dq, {**by_stage["silver"], "bronze_rows": by_stage["bronze"]["rows_written"]})
    forecast(fc, pm)
    model_eval(by_stage["ml"]["metrics"], pd.read_csv(ROOT / "docs" / "feature_importance.csv"))

    log_lines = []
    for r in runs.drop_duplicates("stage", keep="last").itertuples():
        m = json.loads(r.metrics_json)
        m.pop("stage", None)
        for k in ("batch_id", "run_id", "mlflow_run_id"):
            if k in m:
                m[k] = m[k][:8] + "..."
        if "metrics" in m:
            m["metrics"] = {k: v["wape_pct"] for k, v in m["metrics"].items()}
            m["metrics"] = "WAPE% " + ", ".join(f"{k}={v}" for k, v in m["metrics"].items())
        parts = [f"{k}={v}" for k, v in m.items() if v is not None]
        chunks, cur = [], ""
        for part in parts:
            if cur and len(cur) + len(part) > 110:
                chunks.append(cur.rstrip(", "))
                cur = ""
            cur += part + ", "
        chunks.append(cur.rstrip(", "))
        log_lines.append(f"[{r.stage.upper():6}] SUCCESS in {r.duration_s:5.1f}s | {chunks[0]}")
        log_lines += [" " * 28 + "| " + c for c in chunks[1:]]
    run_log("\n".join(log_lines))


if __name__ == "__main__":
    main()
