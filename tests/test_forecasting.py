import numpy as np
import pandas as pd
import pytest

from tradeflow.forecasting import FEATURES, build_features, wape


def _series(n=30):
    dates = pd.date_range("2022-01-01", periods=n, freq="MS")
    return pd.DataFrame({"period_date": dates, "cty_code": "1220", "country_name": "Canada",
                         "flow": "imports", "value_usd": np.linspace(100, 200, n)})


def test_features_do_not_leak_current_value():
    base = build_features(_series())
    changed = _series()
    changed.loc[20, "value_usd"] = 1e9                # change only month 20
    leaked = build_features(changed)
    pd.testing.assert_frame_equal(base.loc[20, FEATURES].to_frame(), leaked.loc[20, FEATURES].to_frame())
    assert leaked.loc[20, "target"] != base.loc[20, "target"]   # only the target sees it


def test_first_year_has_no_features():
    feats = build_features(_series())
    assert feats.loc[:11, "lag_1_ratio"].isna().all()


def test_wape():
    assert wape(np.array([100, 100]), np.array([90, 110])) == pytest.approx(10.0)
