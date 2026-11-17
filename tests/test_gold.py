import datetime as dt

import pytest

from tradeflow.gold import concentration, partner_monthly

COLS = "period_date date, cty_code string, country_name string, iso3 string, region string, " \
       "trade_agreement string, flow string, value_usd double"


def _row(y, flow, v, code="1220"):
    return (dt.date(y, 1, 1), code, "Canada", "CAN", "North America", "USMCA", flow, v)


def test_balance_and_yoy(spark):
    df = spark.createDataFrame(
        [_row(2023, "exports", 100.0), _row(2023, "imports", 200.0),
         _row(2024, "exports", 110.0), _row(2024, "imports", 150.0)], COLS)
    out = {r.period_date.year: r for r in partner_monthly(df).collect()}
    assert out[2024].trade_balance_usd == -40.0
    assert out[2024].exports_yoy_pct == pytest.approx(10.0)
    assert out[2024].imports_yoy_pct == pytest.approx(-25.0)
    assert out[2023].imports_yoy_pct is None          # no prior year


def test_hhi_for_two_equal_partners(spark):
    df = spark.createDataFrame([_row(2024, "imports", 50.0, "1220"), _row(2024, "imports", 50.0, "2010")], COLS)
    row = concentration(df).collect()[0]
    assert row.hhi == pytest.approx(5000.0)           # 50^2 + 50^2
    assert row.partner_count == 2
