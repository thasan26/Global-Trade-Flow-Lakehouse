-- Example analytical queries on the Gold layer (Databricks SQL).
USE CATALOG trade_lakehouse;

-- 1. Top 10 partners by total trade in the latest full year
SELECT country_name, region,
       ROUND(SUM(total_trade_usd) / 1e9, 1)   AS total_trade_bn,
       ROUND(SUM(trade_balance_usd) / 1e9, 1) AS balance_bn
FROM gold.trade_partner_monthly
WHERE year = (SELECT MAX(year) FROM gold.trade_partner_monthly)
GROUP BY country_name, region
ORDER BY total_trade_bn DESC
LIMIT 10;

-- 2. Fastest-growing import sources (latest year vs. prior year)
WITH yearly AS (
  SELECT country_name, year, SUM(imports_usd) AS imports
  FROM gold.trade_partner_monthly GROUP BY country_name, year
)
SELECT cur.country_name,
       ROUND(cur.imports / 1e9, 1)                    AS imports_bn,
       ROUND((cur.imports / prev.imports - 1) * 100, 1) AS growth_pct
FROM yearly cur JOIN yearly prev
  ON cur.country_name = prev.country_name AND cur.year = prev.year + 1
WHERE cur.year = (SELECT MAX(year) FROM yearly)
ORDER BY growth_pct DESC;

-- 3. Sector mix of imports, latest month
SELECT sector, ROUND(SUM(value_usd) / 1e9, 2) AS value_bn, ROUND(SUM(share_of_flow_pct), 1) AS share_pct
FROM gold.trade_sector_monthly
WHERE flow = 'imports' AND period_date = (SELECT MAX(period_date) FROM gold.trade_sector_monthly)
GROUP BY sector ORDER BY value_bn DESC;

-- 4. Is U.S. import sourcing becoming more or less concentrated?
SELECT YEAR(period_date) AS year, ROUND(AVG(hhi), 0) AS avg_hhi, ROUND(AVG(top5_share_pct), 1) AS top5_share
FROM gold.partner_concentration WHERE flow = 'imports'
GROUP BY YEAR(period_date) ORDER BY year;

-- 5. Data-quality trend from the ops log
SELECT DATE(run_ts) AS run_date, rule_name, severity, rows_failed, ROUND(pass_rate * 100, 3) AS pass_pct
FROM ops.dq_results ORDER BY run_ts DESC, rows_failed DESC;
