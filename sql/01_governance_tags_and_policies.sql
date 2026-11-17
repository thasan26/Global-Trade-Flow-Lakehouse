-- =====================================================================
-- Governance applied after the pipeline has created the tables:
-- tags for discovery, column comments, and a row filter.
-- =====================================================================
USE CATALOG trade_lakehouse;

-- ---- Tags (searchable in Catalog Explorer) ---------------------------
ALTER TABLE bronze.trade_flows_raw          SET TAGS ('layer' = 'bronze', 'source' = 'census_intltrade', 'pii' = 'none');
ALTER TABLE silver.trade_flows              SET TAGS ('layer' = 'silver', 'quality' = 'validated', 'pii' = 'none');
ALTER TABLE silver.trade_flows_quarantine   SET TAGS ('layer' = 'silver', 'quality' = 'quarantine');
ALTER TABLE gold.trade_partner_monthly      SET TAGS ('layer' = 'gold', 'consumer' = 'powerbi', 'certified' = 'true');
ALTER TABLE gold.trade_sector_monthly       SET TAGS ('layer' = 'gold', 'consumer' = 'powerbi', 'certified' = 'true');
ALTER TABLE gold.partner_concentration      SET TAGS ('layer' = 'gold', 'consumer' = 'powerbi');
ALTER TABLE gold.trade_forecast             SET TAGS ('layer' = 'gold', 'consumer' = 'powerbi', 'model' = 'trade_forecaster');

-- ---- Column documentation --------------------------------------------
ALTER TABLE gold.trade_partner_monthly ALTER COLUMN trade_balance_usd COMMENT 'Exports minus imports (USD). Negative = deficit.';
ALTER TABLE gold.trade_partner_monthly ALTER COLUMN imports_yoy_pct   COMMENT 'Imports growth vs. same month of the prior year (%).';
ALTER TABLE gold.partner_concentration ALTER COLUMN hhi               COMMENT 'Herfindahl-Hirschman Index of partner shares (0-10,000). >2,500 = highly concentrated.';

-- ---- Row filter example ----------------------------------------------
-- A regional analyst group sees only its own region in the Gold partner table.
CREATE OR REPLACE FUNCTION ops.region_filter(region STRING)
RETURN is_account_group_member('data_engineers')
    OR is_account_group_member('analysts')
    OR (is_account_group_member('analysts_europe') AND region = 'Europe');

ALTER TABLE gold.trade_partner_monthly SET ROW FILTER ops.region_filter ON (region);

-- ---- Lineage ---------------------------------------------------------
-- Unity Catalog captures table- and column-level lineage automatically for
-- every notebook and job run: open gold.trade_partner_monthly in Catalog
-- Explorer -> Lineage to see bronze -> silver -> gold.
