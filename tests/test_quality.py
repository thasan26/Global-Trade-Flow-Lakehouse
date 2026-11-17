from tradeflow.quality import Rule, apply_rules, split_valid_quarantine, summarize

RULES = [
    Rule("not_null", "v IS NOT NULL", "error", ""),
    Rule("non_negative", "v IS NULL OR v >= 0", "error", ""),
    Rule("small", "v IS NULL OR v < 100", "warn", ""),
]


def test_rules_route_bad_rows_to_quarantine(spark):
    df = spark.createDataFrame([(1, 10.0), (2, None), (3, -5.0), (4, 500.0)], "id int, v double")
    valid, quarantine = split_valid_quarantine(apply_rules(df, RULES))

    assert sorted(r.id for r in valid.collect()) == [1, 4]          # warn-only failure is kept
    reasons = {r.id: r._dq_errors for r in quarantine.collect()}
    assert reasons[2] == ["not_null"]                               # NULL counts as a failure
    assert reasons[3] == ["non_negative"]


def test_warnings_are_recorded_not_dropped(spark):
    df = spark.createDataFrame([(1, 500.0)], "id int, v double")
    row = apply_rules(df, RULES).collect()[0]
    assert row._dq_errors == [] and row._dq_warnings == ["small"]


def test_summary_counts_failures_per_rule(spark):
    df = spark.createDataFrame([(1, 10.0), (2, None), (3, -5.0), (4, 500.0)], "id int, v double")
    summary = {r.rule_name: r for r in summarize(spark, apply_rules(df, RULES), RULES, "t", "run").collect()}
    assert summary["not_null"].rows_failed == 1
    assert summary["non_negative"].rows_failed == 1
    assert summary["small"].rows_failed == 1
    assert summary["not_null"].pass_rate == 0.75
