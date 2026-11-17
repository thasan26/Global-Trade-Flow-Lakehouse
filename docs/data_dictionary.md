# Data dictionary

All tables live in Unity Catalog under `trade_lakehouse.<schema>.<table>`.

## bronze.trade_flows_raw
Raw records exactly as received. Every business field is a string. Partitioned by `flow`.

| Column | Type | Description |
|---|---|---|
| flow | string | `exports` or `imports` |
| period | string | Month, `YYYY-MM` |
| cty_code | string | Census partner-country code (Schedule C) |
| cty_name | string | Partner name as sent by the source |
| commodity_code | string | HS2 chapter code |
| commodity_desc | string | HS2 chapter description |
| value_usd | string | Monthly trade value in USD (exports: `ALL_VAL_MO`, imports: `GEN_VAL_MO`) |
| _source_file | string | Landing file the row came from |
| _file_modified_at | timestamp | Modification time of that file (used to pick the latest revision) |
| _ingested_at | timestamp | When the row was loaded |
| _batch_id | string | Load batch identifier |

## silver.trade_flows
Validated, deduplicated, conformed records. Partitioned by `flow`.

| Column | Type | Description |
|---|---|---|
| flow, period | string | As above, cleaned |
| period_date | date | First day of the month |
| cty_code, country_name, iso3 | string | Partner keys from `dim_country` |
| region, trade_agreement | string | Partner attributes |
| commodity_code, chapter_description, sector | string | Commodity attributes from `dim_hs_chapter` |
| value_usd | double | Monthly value in USD |
| _dq_warnings | array<string> | Warning-level rules the row failed (row is kept) |
| _source_file, _ingested_at, _batch_id | | Lineage carried from Bronze |

## silver.trade_flows_quarantine
Rows that failed at least one error-level rule. `_dq_errors` lists every rule the row failed.

## silver.dim_country / silver.dim_hs_chapter
Reference data loaded from `data/reference/*.csv`.

## gold.trade_partner_monthly
| Column | Description |
|---|---|
| period_date, year, month | Month |
| cty_code, country_name, iso3, region, trade_agreement | Partner |
| exports_usd, imports_usd, total_trade_usd | Monthly values |
| trade_balance_usd | exports - imports |
| exports_yoy_pct, imports_yoy_pct | Growth vs. the same month a year earlier |

## gold.trade_sector_monthly
`period_date, flow, sector, commodity_code, chapter_description, value_usd, share_of_flow_pct`

## gold.partner_concentration
`period_date, flow, hhi, top5_share_pct, partner_count`. HHI = sum of squared partner shares (in %), 0-10,000.

## gold.trade_kpis_annual
`year, flow, total_usd, months_reported, yoy_growth_pct, top_partner, top_partner_usd, top_partner_share_pct`

## gold.trade_forecast
| Column | Description |
|---|---|
| period_date, cty_code, country_name, flow | Series and month |
| actual_usd | Observed value (backtest rows only) |
| forecast_usd | Model prediction |
| baseline_usd | Seasonal-naive prediction for comparison |
| split | `backtest` (one-step-ahead on the test year) or `forecast` (future months) |
| abs_pct_error | Absolute % error (backtest rows) |
| mlflow_run_id | MLflow run that produced the row |

## ml.model_metrics
`mlflow_run_id, model, wape_pct, mape_pct` - one row per model per training run.

## ops.dq_results
`run_id, run_ts, table_name, rule_name, severity, description, rows_checked, rows_failed, pass_rate`

## ops.pipeline_runs
`stage, status, started_at, duration_s, metrics_json`
