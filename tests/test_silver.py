import datetime as dt

from tradeflow.silver import deduplicate, standardize

COLS = ("flow string, period string, cty_code string, cty_name string, commodity_code string, "
        "commodity_desc string, value_usd string, _source_file string, _file_modified_at timestamp, "
        "_ingested_at timestamp, _batch_id string")
T0 = dt.datetime(2025, 1, 1)
T1 = dt.datetime(2025, 1, 2)


def test_standardize_casts_and_cleans(spark):
    df = spark.createDataFrame(
        [(" Imports ", "2024-03", "5700", " China  ", "8", "MACHINERY", " 1200 ", "f", T0, T0, "b")], COLS)
    row = standardize(df).collect()[0]
    assert row.flow == "imports"
    assert row.cty_name_clean == "CHINA"
    assert row.commodity_code == "08"                 # zero-padded HS2
    assert int(row.value_usd) == 1200
    assert row.period_date == dt.date(2024, 3, 1)


def test_empty_value_becomes_null(spark):
    df = spark.createDataFrame([("exports", "2024-03", "1220", "CANADA", "27", "FUELS", "", "f", T0, T0, "b")], COLS)
    assert standardize(df).collect()[0].value_usd is None


def test_dedup_keeps_latest_revision(spark):
    rows = [
        ("imports", "2024-03", "5700", "CHINA", "84", "M", "100", "orig", T0, T0, "b"),
        ("imports", "2024-03", "5700", "CHINA", "84", "M", "105", "revision", T1, T0, "b"),
        ("imports", "2024-04", "5700", "CHINA", "84", "M", "90", "orig", T0, T0, "b"),
    ]
    current, superseded = deduplicate(standardize(spark.createDataFrame(rows, COLS)))
    values = {r.period: int(r.value_usd) for r in current.collect()}
    assert values == {"2024-03": 105, "2024-04": 90}
    assert superseded.count() == 1
