-- =====================================================================
-- Global Trade Flow Lakehouse - Unity Catalog setup
-- Run once in a Databricks SQL editor or notebook by a user with
-- CREATE CATALOG rights (in Databricks Free Edition, your own account).
-- =====================================================================

CREATE CATALOG IF NOT EXISTS trade_lakehouse
  COMMENT 'U.S. international trade lakehouse: medallion layers, ML and ops metadata';

USE CATALOG trade_lakehouse;

-- ---------------------------------------------------------------------
-- Medallion schemas
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw records exactly as received, with lineage columns';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Typed, deduplicated, validated and conformed records';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Business-ready aggregates for BI and ML';
CREATE SCHEMA IF NOT EXISTS ml     COMMENT 'Registered models and model-evaluation tables';
CREATE SCHEMA IF NOT EXISTS ops    COMMENT 'Data-quality results, run logs and stream checkpoints';

-- ---------------------------------------------------------------------
-- Volumes: governed storage for raw files and Auto Loader checkpoints
-- ---------------------------------------------------------------------
CREATE VOLUME IF NOT EXISTS bronze.landing     COMMENT 'Landing zone for raw Census trade files (NDJSON)';
CREATE VOLUME IF NOT EXISTS ops.checkpoints    COMMENT 'Structured Streaming / Auto Loader checkpoints';

-- ---------------------------------------------------------------------
-- Access control (least privilege). Replace group names with your own.
--   data_engineers : build and maintain every layer
--   analysts       : read Gold only (Power BI service principal goes here)
--   data_scientists: read Silver/Gold, write to ml
-- ---------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG trade_lakehouse TO `data_engineers`;
GRANT USE CATALOG ON CATALOG trade_lakehouse TO `analysts`;
GRANT USE CATALOG ON CATALOG trade_lakehouse TO `data_scientists`;

GRANT ALL PRIVILEGES ON SCHEMA bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA silver TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA gold   TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA ops    TO `data_engineers`;

GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `analysts`;

GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `data_scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `data_scientists`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MODEL ON SCHEMA ml TO `data_scientists`;
