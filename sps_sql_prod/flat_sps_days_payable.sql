-- This table aggregates days payable metrics for generating Supplier Scorecards.
-- SPS Execution: Position No. 9.2
-- Ingredientes añadidos:
--   SUM(sku_month_end_stock_value_eur) AS stock_value_eur
--   SUM(sku_cogs_eur_monthy)           AS cogs_monthly_eur
--   MAX(days_in_month)                 AS days_in_month
--   MAX(days_in_quarter)               AS days_in_quarter
-- doh y dpo se mantienen para compatibilidad
-- En Tableau:
--   doh_monthly  = SUM(stock_value_eur) / (SUM(cogs_monthly_eur) / MAX(days_in_month))
--   doh_quarterly= SUM(stock_value_eur) / (SUM(cogs_monthly_eur) / (MAX(days_in_quarter)/3))
--   dpo          = MAX(payment_days) - doh
-- ============================================================

-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'FP_HK|FP_PH|FP_SG|GV_ES|GV_IT|GV_UA|HF_EG|HS_SA|IN_AE|IN_EG|NP_HU|PY_AR|PY_CL|PY_PE|TB_AE|TB_BH|TB_JO|TB_KW|TB_OM|TB_QA|YS_TR';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-10-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_days_payable`
CLUSTER BY
   global_entity_id, 
   time_period
AS 
WITH date_config AS (
  SELECT 
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 4 QUARTER) AS lookback_limit
)
SELECT
  global_entity_id,
  CASE WHEN GROUPING(stock_days_month) = 0 THEN CAST(stock_days_month AS STRING) ELSE stock_days_quarter END AS time_period,
  CASE 
      WHEN GROUPING(principal_supplier_id) = 0 THEN principal_supplier_id
      WHEN GROUPING(supplier_id) = 0 THEN supplier_id
      WHEN GROUPING(brand_owner_name) = 0 THEN brand_owner_name
      WHEN GROUPING(brand_name) = 0 THEN brand_name
      ELSE 'total'
  END AS brand_sup,
  COALESCE(
    IF(GROUPING(l3_master_category) = 0, l3_master_category, NULL),
    IF(GROUPING(l2_master_category) = 0, l2_master_category, NULL),
    IF(GROUPING(l1_master_category) = 0, l1_master_category, NULL),
    IF(GROUPING(brand_name) = 0, brand_name, NULL),
    IF(GROUPING(brand_owner_name) = 0, brand_owner_name, NULL),
    IF(GROUPING(supplier_id) = 0, supplier_id, NULL),
    principal_supplier_id
  ) AS entity_key,
  CASE 
      WHEN GROUPING(principal_supplier_id) = 0 THEN 'principal' 
      WHEN GROUPING(supplier_id) = 0 THEN 'division' 
      WHEN GROUPING(brand_owner_name) = 0 THEN 'brand_owner' 
      WHEN GROUPING(brand_name) = 0 THEN 'brand_name'
      ELSE 'total'
  END AS division_type,
  CASE 
      WHEN GROUPING(l3_master_category) = 0 THEN 'level_three' 
      WHEN GROUPING(l2_master_category) = 0 THEN 'level_two' 
      WHEN GROUPING(l1_master_category) = 0 THEN 'level_one' 
      WHEN GROUPING(brand_name) = 0 THEN 'brand_name'
      ELSE 'supplier' 
  END AS supplier_level,
  CASE WHEN GROUPING(stock_days_month) = 0 THEN 'Monthly' ELSE 'Quarterly' END AS time_granularity,
  MAX(payment_days) AS payment_days,
  -- Ingredientes añadidos
  SUM(sku_month_end_stock_value_eur) AS stock_value_eur,
  SUM(sku_cogs_eur_monthy)           AS cogs_monthly_eur,
  MAX(days_in_month)                 AS days_in_month,
  MAX(days_in_quarter)               AS days_in_quarter,
  -- Ratios pre-calculados (mantener para compatibilidad)
  CASE
    WHEN GROUPING(stock_days_month) = 0 THEN SAFE_DIVIDE(SUM(sku_month_end_stock_value_eur), SAFE_DIVIDE(SUM(sku_cogs_eur_monthy), MAX(days_in_month)))
    ELSE SAFE_DIVIDE(SUM(sku_month_end_stock_value_eur), SAFE_DIVIDE(SUM(sku_cogs_eur_monthy), MAX(days_in_quarter) / 3))
  END AS doh,
  CASE
    WHEN GROUPING(stock_days_month) = 0 THEN SAFE_SUBTRACT(MAX(payment_days), SAFE_DIVIDE(SUM(sku_month_end_stock_value_eur), SAFE_DIVIDE(SUM(sku_cogs_eur_monthy), MAX(days_in_month))))
    ELSE SAFE_SUBTRACT(MAX(payment_days), SAFE_DIVIDE(SUM(sku_month_end_stock_value_eur), SAFE_DIVIDE(SUM(sku_cogs_eur_monthy), MAX(days_in_quarter) / 3)))
  END AS dpo,
FROM `dh-darkstores-live.csm_automated_tables.sps_days_payable_month`
WHERE CAST(stock_days_month AS DATE) >= (SELECT lookback_limit FROM date_config)
GROUP BY GROUPING SETS (
    -- ==========================================================
    -- MONTHLY BREAKDOWNS (month)
    -- ==========================================================
    
    -- 1. TOTAL OWNER LEVEL (No Category/Brand Deep-dive)
    (stock_days_month, global_entity_id, principal_supplier_id),
    (stock_days_month, global_entity_id, supplier_id),
    (stock_days_month, global_entity_id, brand_owner_name),

    -- 2. BRAND DEEP-DIVE (By Owner + Brand Name)
    (stock_days_month, global_entity_id, principal_supplier_id, brand_name),
    (stock_days_month, global_entity_id, supplier_id, brand_name),
    (stock_days_month, global_entity_id, brand_owner_name, brand_name),
    (stock_days_month, global_entity_id, brand_name), 

    -- 3. CATEGORY DEEP-DIVE (By Owner + Categories)
    (stock_days_month, global_entity_id, principal_supplier_id, l1_master_category),
    (stock_days_month, global_entity_id, principal_supplier_id, l2_master_category),
    (stock_days_month, global_entity_id, principal_supplier_id, l3_master_category),

    (stock_days_month, global_entity_id, supplier_id, l1_master_category),
    (stock_days_month, global_entity_id, supplier_id, l2_master_category),
    (stock_days_month, global_entity_id, supplier_id, l3_master_category),

    (stock_days_month, global_entity_id, brand_owner_name, l1_master_category),
    (stock_days_month, global_entity_id, brand_owner_name, l2_master_category),
    (stock_days_month, global_entity_id, brand_owner_name, l3_master_category),

    (stock_days_month, global_entity_id, brand_name, l1_master_category),
    (stock_days_month, global_entity_id, brand_name, l2_master_category),
    (stock_days_month, global_entity_id, brand_name, l3_master_category),

    -- ==========================================================
    -- QUARTERLY BREAKDOWNS (quarter_year)
    -- ==========================================================
    
    -- 1. TOTAL OWNER LEVEL
    (stock_days_quarter, global_entity_id, principal_supplier_id),
    (stock_days_quarter, global_entity_id, supplier_id),
    (stock_days_quarter, global_entity_id, brand_owner_name),

    -- 2. BRAND DEEP-DIVE
    (stock_days_quarter, global_entity_id, principal_supplier_id, brand_name),
    (stock_days_quarter, global_entity_id, supplier_id, brand_name),
    (stock_days_quarter, global_entity_id, brand_owner_name, brand_name),
    (stock_days_quarter, global_entity_id, brand_name),

    -- 3. CATEGORY DEEP-DIVE
    (stock_days_quarter, global_entity_id, principal_supplier_id, l1_master_category),
    (stock_days_quarter, global_entity_id, principal_supplier_id, l2_master_category),
    (stock_days_quarter, global_entity_id, principal_supplier_id, l3_master_category),

    (stock_days_quarter, global_entity_id, supplier_id, l1_master_category),
    (stock_days_quarter, global_entity_id, supplier_id, l2_master_category),
    (stock_days_quarter, global_entity_id, supplier_id, l3_master_category),

    (stock_days_quarter, global_entity_id, brand_owner_name, l1_master_category),
    (stock_days_quarter, global_entity_id, brand_owner_name, l2_master_category),
    (stock_days_quarter, global_entity_id, brand_owner_name, l3_master_category),

    (stock_days_quarter, global_entity_id, brand_name, l1_master_category),
    (stock_days_quarter, global_entity_id, brand_name, l2_master_category),
    (stock_days_quarter, global_entity_id, brand_name, l3_master_category)
);