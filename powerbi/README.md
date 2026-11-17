# Power BI dashboard

The Gold tables are designed to be read directly by Power BI.

## Connect
1. In Databricks, open **SQL Warehouses → your warehouse → Connection details** and copy the *Server hostname* and *HTTP path*.
2. In Power BI Desktop: **Get Data → Azure Databricks** (the connector also works for AWS and GCP workspaces).
3. Paste the hostname and HTTP path, sign in, and select from `trade_lakehouse.gold`:
   `trade_partner_monthly`, `trade_sector_monthly`, `partner_concentration`, `trade_forecast`,
   plus `trade_lakehouse.silver.dim_country`.
4. Use **Import** mode (the tables are small and refresh monthly).

## Model
- Create the `Date` table and all measures from [`measures.dax`](measures.dax).
- Relationships are listed at the top of that file. Mark `Date` as the date table.

## Report pages
| Page | Visuals |
|---|---|
| Executive overview | KPI cards (Exports $B, Imports $B, Trade Balance $B, Imports YoY %), line chart of Exports vs Imports by month, map by `iso3` |
| Partners | Bar chart of Total Trade by partner, matrix with Rank / Share % / YoY %, slicer on region and trade agreement |
| Sectors | Stacked bar of Sector Value by year, treemap of HS chapters, Sector Share % |
| Concentration | Line chart of Import HHI with the 1,500 / 2,500 thresholds, Top 5 Partner Share % |
| Forecast | Line chart of Actual vs Forecast by partner, Forecast WAPE % card, Next 3M Forecast table |

## Screenshot
After building the report, export a screenshot to `docs/images/powerbi_dashboard.png` and add it to the main README.
