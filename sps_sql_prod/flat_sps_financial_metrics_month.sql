-- This table extracts and maintains the financial metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 5.1
-- DML SCRIPT: SPS Refact Incremental Refresh for dh-darkstores-live.csm_automated_tables.sps_financial_metrics_month
CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_month`
AS
WITH
date_in AS (
  SELECT DATE('2025-10-01') AS date_in
),
date_fin AS (
  SELECT CURRENT_DATE() AS date_fin
)
  SELECT
    os.global_entity_id,
    os.country_code,
    os.order_date,
    COALESCE(os.brand_name, '_unknown_') AS brand_name, 
    COALESCE(os.brand_owner_name, '_unknown_') AS brand_owner_name, 
    COALESCE(CAST(os.supplier_id AS STRING), '_unknown_') AS supplier_id,
    -- os.supplier_id,
    --  COALESCE(CAST(ANY_VALUE(os.sup_id_parent) AS STRING), '_unknown_') AS principal_supplier_id,
    os.sup_id_parent AS principal_supplier_id,
    -- os.is_sup_id_parent,
    os.supplier_name,
    CAST(DATE_TRUNC(os.order_date, MONTH) AS STRING) AS month,
    CAST(CONCAT('Q', EXTRACT(QUARTER FROM os.order_date), '-', EXTRACT(YEAR FROM os.order_date)) AS STRING) AS quarter_year,
    COALESCE(os.level_one, '_unknown_') AS l1_master_category,
    COALESCE(os.level_two, '_unknown_') AS l2_master_category,
    COALESCE(os.level_three, '_unknown_') AS l3_master_category,
    os.order_id,
    os.order_status,
    os.ordered_quantity,
    os.delivered_quantity,
    os.sku_id,
    os.warehouse_id,
    os.analytical_customer_id,
    ---- EUR ----
    os.total_price_paid_net_eur,
    os.amt_total_price_paid_net_eur,
    os.COGS_eur,
    os.unit_discount_amount_eur,
    os.total_supplier_funding_eur,
    os.total_discount_eur,
    ---- LC ----
    os.total_price_paid_net_lc,
    os.amt_total_price_paid_net_lc,
    os.COGS_lc,
    os.unit_discount_amount_lc,
    os.total_supplier_funding_lc,
    os.total_discount_lc,
    os.fulfilled_quantity,
    os.amt_gbv_eur, 
    CASE
      WHEN DATE_TRUNC(CAST(os.order_date AS DATE), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
      THEN CURRENT_DATE()
        ELSE LAST_DAY(CAST(os.order_date AS DATE))
    END AS partition_month,
  FROM `dh-darkstores-live.csm_automated_tables.sps_customer_order` AS os
  WHERE TRUE
    AND os.country_code = 'pe'
    AND (os.order_date BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)