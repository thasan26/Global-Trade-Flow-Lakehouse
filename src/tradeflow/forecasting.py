"""Trade-volume forecasting with MLflow tracking.

Problem
    Forecast next month's exports and imports for each partner country
    (25 partners x 2 flows = 50 monthly series).

Approach
    One global gradient-boosting model learns from all 50 series at once.
    It predicts the *seasonal log-ratio* log(y_t / y_{t-12}), which makes the
    target scale-free, so a $40B/month partner and a $1B/month partner can
    share one model. The prediction is converted back to dollars as
    y_{t-12} * exp(prediction).

Validation
    Chronological split - train on months up to ``train_end_period``, test on
    the following 12 months, one step ahead (no future data leaks into
    features). The model is compared against two standard baselines:
    last-month naive and seasonal naive (same month last year).

Tracking
    Parameters, metrics, feature importance and the fitted model are logged
    to MLflow. On Databricks the model is registered in Unity Catalog as
    ``<catalog>.ml.trade_forecaster``.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile

import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance

from tradeflow.config import PipelineConfig, is_databricks
from tradeflow.io import read_table, write_table

log = logging.getLogger(__name__)

FEATURES = [
    "lag_1_ratio", "lag_2_ratio", "lag_3_ratio",        # recent level vs. same month last year
    "mom_change_1", "mom_change_2",                       # month-over-month momentum
    "roll_mean_3_ratio", "roll_mean_6_ratio", "roll_std_6", # smoothed level and volatility
    "yoy_ratio_lag1",                                     # last observed YoY growth
    "month_sin", "month_cos",                             # seasonality
    "is_imports", "log_scale",                            # series descriptors
]


# --------------------------------------------------------------------------
# Data preparation
# --------------------------------------------------------------------------
def to_long(partner_monthly: pd.DataFrame) -> pd.DataFrame:
    df = partner_monthly.melt(
        id_vars=["period_date", "cty_code", "country_name"],
        value_vars=["exports_usd", "imports_usd"], var_name="flow", value_name="value_usd",
    )
    df["flow"] = df["flow"].str.replace("_usd", "", regex=False)
    df["period_date"] = pd.to_datetime(df["period_date"])
    df = df[df["value_usd"] > 0]
    return df.sort_values(["cty_code", "flow", "period_date"]).reset_index(drop=True)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Features for predicting month t use only values from t-1 and earlier."""
    out = df.copy()
    out["log_v"] = np.log(out["value_usd"])
    g = out.groupby(["cty_code", "flow"], sort=False)["log_v"]

    lag = {k: g.shift(k) for k in (1, 2, 3, 12, 13, 14, 15)}
    out["log_lag12"] = lag[12]
    out["lag_1_ratio"] = lag[1] - lag[12]
    out["lag_2_ratio"] = lag[2] - lag[12]
    out["lag_3_ratio"] = lag[3] - lag[12]
    out["mom_change_1"] = lag[1] - lag[2]
    out["mom_change_2"] = lag[2] - lag[3]
    shifted = g.shift(1)
    grp = shifted.groupby([out["cty_code"], out["flow"]], sort=False)
    out["roll_mean_3_ratio"] = grp.transform(lambda s: s.rolling(3).mean()) - lag[12]
    out["roll_mean_6_ratio"] = grp.transform(lambda s: s.rolling(6).mean()) - lag[12]
    out["roll_std_6"] = grp.transform(lambda s: s.diff().rolling(6).std())
    out["yoy_ratio_lag1"] = lag[1] - lag[13]
    m = out["period_date"].dt.month
    out["month_sin"] = np.sin(2 * np.pi * m / 12)
    out["month_cos"] = np.cos(2 * np.pi * m / 12)
    out["is_imports"] = (out["flow"] == "imports").astype(int)
    out["log_scale"] = grp.transform(lambda s: s.rolling(12).mean())
    out["target"] = out["log_v"] - out["log_lag12"]
    return out


# --------------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------------
def wape(y, yhat) -> float:
    return float(np.sum(np.abs(y - yhat)) / np.sum(np.abs(y)) * 100)


def mape(y, yhat) -> float:
    return float(np.mean(np.abs((y - yhat) / y)) * 100)


def evaluate(y, yhat) -> dict[str, float]:
    return {"wape_pct": round(wape(y, yhat), 3), "mape_pct": round(mape(y, yhat), 3)}


# --------------------------------------------------------------------------
# Training
# --------------------------------------------------------------------------
def make_model(random_state: int) -> HistGradientBoostingRegressor:
    return HistGradientBoostingRegressor(
        learning_rate=0.05, max_iter=400, max_leaf_nodes=15, min_samples_leaf=40,
        l2_regularization=1.0, early_stopping=False, random_state=random_state,
    )


def recursive_forecast(model, history: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Forecast `horizon` months past the last observed month, feeding predictions back in."""
    hist = history[["period_date", "cty_code", "country_name", "flow", "value_usd"]].copy()
    rows = []
    for _ in range(horizon):
        nxt = hist.groupby(["cty_code", "country_name", "flow"], as_index=False)["period_date"].max()
        nxt["period_date"] = nxt["period_date"] + pd.offsets.MonthBegin(1)
        nxt["value_usd"] = np.nan
        frame = build_features(pd.concat([hist, nxt], ignore_index=True))
        step = frame[frame["value_usd"].isna()].copy()
        step["forecast_usd"] = np.exp(step["log_lag12"] + model.predict(step[FEATURES]))
        rows.append(step[["period_date", "cty_code", "country_name", "flow", "forecast_usd"]])
        hist = pd.concat([hist, step.assign(value_usd=step["forecast_usd"])[hist.columns]], ignore_index=True)
    return pd.concat(rows, ignore_index=True)


def _setup_mlflow(cfg: PipelineConfig) -> None:
    if is_databricks():
        mlflow.set_registry_uri("databricks-uc")
        mlflow.set_experiment(cfg.ml["experiment_name"])
    else:
        mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", f"sqlite:///{cfg.root / 'mlflow.db'}"))
        mlflow.set_experiment(cfg.ml["experiment_name"].rsplit("/", 1)[-1])


def train_and_forecast(spark, cfg: PipelineConfig) -> dict:
    ml = cfg.ml
    pdf = read_table(spark, cfg, "gold", "trade_partner_monthly").select(
        "period_date", "cty_code", "country_name", "exports_usd", "imports_usd"
    ).toPandas()
    long = to_long(pdf)
    feats = build_features(long).dropna(subset=FEATURES + ["target"])

    train_end = pd.Timestamp(ml["train_end_period"] + "-01")
    test_end = pd.Timestamp(ml["test_end_period"] + "-01")
    train = feats[feats["period_date"] <= train_end]
    test = feats[(feats["period_date"] > train_end) & (feats["period_date"] <= test_end)].copy()

    _setup_mlflow(cfg)
    with mlflow.start_run(run_name="hgb_global_seasonal_ratio") as run:
        model = make_model(ml["random_state"])
        model.fit(train[FEATURES], train["target"])

        test["pred_model"] = np.exp(test["log_lag12"] + model.predict(test[FEATURES]))
        test["pred_naive"] = np.exp(test["lag_1_ratio"] + test["log_lag12"])   # = last month's value
        test["pred_seasonal_naive"] = np.exp(test["log_lag12"])
        y = test["value_usd"].to_numpy()

        metrics = {
            "model": evaluate(y, test["pred_model"].to_numpy()),
            "naive_last_month": evaluate(y, test["pred_naive"].to_numpy()),
            "seasonal_naive": evaluate(y, test["pred_seasonal_naive"].to_numpy()),
        }
        for name, vals in metrics.items():
            for k, v in vals.items():
                mlflow.log_metric(f"{name}_{k}", v)
        best_baseline = min(metrics["naive_last_month"]["wape_pct"], metrics["seasonal_naive"]["wape_pct"])
        improvement = round((1 - metrics["model"]["wape_pct"] / best_baseline) * 100, 2)
        mlflow.log_metric("wape_improvement_vs_best_baseline_pct", improvement)

        mlflow.log_params({
            **{k: v for k, v in model.get_params().items() if k in
               ("learning_rate", "max_iter", "max_leaf_nodes", "min_samples_leaf", "l2_regularization")},
            "target": "log(y_t / y_t-12)", "n_series": long.groupby(["cty_code", "flow"]).ngroups,
            "train_rows": len(train), "test_rows": len(test),
            "train_end_period": ml["train_end_period"], "test_end_period": ml["test_end_period"],
            "features": ",".join(FEATURES),
        })

        imp = permutation_importance(model, test[FEATURES], test["target"], n_repeats=5,
                                     random_state=ml["random_state"], scoring="neg_mean_absolute_error")
        importance = (pd.DataFrame({"feature": FEATURES, "importance": imp.importances_mean})
                      .sort_values("importance", ascending=False))
        with tempfile.TemporaryDirectory() as tmp:
            importance.to_csv(os.path.join(tmp, "feature_importance.csv"), index=False)
            with open(os.path.join(tmp, "backtest_metrics.json"), "w") as fh:
                json.dump(metrics, fh, indent=2)
            mlflow.log_artifacts(tmp, artifact_path="evaluation")

        # Refit on all history (train + test) before forecasting the future.
        final = make_model(ml["random_state"])
        all_hist = feats[feats["period_date"] <= test_end]
        final.fit(all_hist[FEATURES], all_hist["target"])
        signature = infer_signature(all_hist[FEATURES], final.predict(all_hist[FEATURES].head(50)))
        registered = (f"{cfg.catalog}.{cfg.schema('ml')}.{ml['registered_model_name']}"
                      if is_databricks() else None)
        mlflow.sklearn.log_model(final, name="model", signature=signature,
                                 input_example=all_hist[FEATURES].head(3),
                                 registered_model_name=registered,
                                 serialization_format="cloudpickle")

        forward = recursive_forecast(final, long[long["period_date"] <= test_end], ml["forecast_horizon_months"])
        run_id = run.info.run_id

    backtest = test[["period_date", "cty_code", "country_name", "flow", "value_usd", "pred_model",
                     "pred_seasonal_naive"]].rename(columns={"value_usd": "actual_usd", "pred_model": "forecast_usd",
                                                             "pred_seasonal_naive": "baseline_usd"})
    backtest["split"] = "backtest"
    forward = forward.assign(actual_usd=np.nan, baseline_usd=np.nan, split="forecast")
    out = pd.concat([backtest, forward[backtest.columns]], ignore_index=True)
    out["abs_pct_error"] = (out["forecast_usd"] - out["actual_usd"]).abs() / out["actual_usd"] * 100
    out["mlflow_run_id"] = run_id
    out["period_date"] = out["period_date"].dt.date

    write_table(spark.createDataFrame(out), cfg, "gold", "trade_forecast",
                comment="One-step backtest and forward forecasts of monthly trade by partner and flow.")
    metric_rows = [(run_id, name, v["wape_pct"], v["mape_pct"]) for name, v in metrics.items()]
    write_table(spark.createDataFrame(metric_rows, "mlflow_run_id string, model string, wape_pct double, "
                                                   "mape_pct double"),
                cfg, "ml", "model_metrics", mode="append", comment="Backtest metrics per training run.")
    importance.to_csv(cfg.root / "docs" / "feature_importance.csv", index=False)

    return {"stage": "ml", "mlflow_run_id": run_id, "metrics": metrics,
            "wape_improvement_vs_best_baseline_pct": improvement,
            "forecast_rows": int((out["split"] == "forecast").sum()), "registered_model": registered}
