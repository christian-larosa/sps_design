CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.ytd_sps_market_yoy`
AS
SELECT
  global_entity_id,
  time_period,
  time_granularity,
  ROUND(SAFE_DIVIDE(
    SUM(Net_Sales_lc) - SUM(Net_Sales_lc_Last_Year),
    NULLIF(SUM(Net_Sales_lc_Last_Year), 0)
  ), 4) AS market_yoy_lc
FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_score_tableau`
WHERE supplier_level    = 'supplier'
  AND division_type     = 'division'
  AND Net_Sales_eur     > 1000
  AND Net_Sales_eur_Last_Year > 0
GROUP BY global_entity_id, time_period, time_granularity
