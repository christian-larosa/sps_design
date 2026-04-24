-- This table aggregates line rebate metrics for generating Supplier Scorecards.
-- SPS Execution: Position No. 4.2
-- Ingredientes añadidos en grouping sets:
--   SUM(sku_calc_gross_delivered) AS calc_gross_delivered
--   SUM(sku_calc_gross_return)    AS calc_gross_return
--   SUM(sku_calc_net_delivered)   AS calc_net_delivered
--   SUM(sku_calc_net_return)      AS calc_net_return
-- net_purchase y total_rebate se mantienen para compatibilidad

-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'FP_HK|FP_PH|FP_SG|GV_ES|GV_IT|GV_UA|HF_EG|HS_SA|IN_AE|IN_EG|NP_HU|PY_AR|PY_CL|PY_PE|TB_AE|TB_BH|TB_JO|TB_KW|TB_OM|TB_QA|YS_TR';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-10-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_line_rebate_metrics`
CLUSTER BY global_entity_id, time_period
AS
WITH date_config AS (
  SELECT
  DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 4 QUARTER) AS lookback_limit
),
base AS (
  SELECT
    DATE(month) AS month_date,
    month,
    quarter_year,
    global_entity_id,
    supplier_id,
    principal_supplier_id,
    brand_name,
    brand_owner_name,
    l1_master_category,
    l2_master_category,
    l3_master_category,
    sku_calc_gross_delivered,
    sku_calc_gross_return,
    sku_calc_net_delivered,
    sku_calc_net_return,
    sku_rebate,
    sku_rebate_wo_dist_allowance_lc
  FROM `dh-darkstores-live.csm_automated_tables.sps_line_rebate_metrics_month`
),
filtered AS (
  SELECT b.*
  FROM base b
  JOIN date_config d
    ON b.month_date >= d.lookback_limit
)
SELECT
  global_entity_id,
  CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING) ELSE quarter_year END AS time_period,
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
  CASE WHEN GROUPING(month) = 0 THEN 'Monthly' ELSE 'Quarterly' END AS time_granularity,
  ROUND(SUM(sku_calc_net_delivered - sku_calc_net_return), 2) AS net_purchase,
  ROUND(SUM(sku_rebate), 2) AS total_rebate,
  ROUND(SUM(sku_rebate_wo_dist_allowance_lc), 4) AS total_rebate_wo_dist_allowance_lc,
  -- Ingredientes añadidos
  ROUND(SUM(sku_calc_gross_delivered), 2) AS calc_gross_delivered,
  ROUND(SUM(sku_calc_gross_return), 2)    AS calc_gross_return,
  ROUND(SUM(sku_calc_net_delivered), 2)   AS calc_net_delivered,
  ROUND(SUM(sku_calc_net_return), 2)      AS calc_net_return
FROM filtered
GROUP BY GROUPING SETS (
    -- ==========================================================
    -- MONTHLY BREAKDOWNS (month)
    -- ==========================================================
    
    -- 1. TOTAL OWNER LEVEL (No Category/Brand Deep-dive)
    (month, global_entity_id, principal_supplier_id),
    (month, global_entity_id, supplier_id),
    (month, global_entity_id, brand_owner_name),

    -- 2. BRAND DEEP-DIVE (By Owner + Brand Name)
    (month, global_entity_id, principal_supplier_id, brand_name),
    (month, global_entity_id, supplier_id, brand_name),
    (month, global_entity_id, brand_owner_name, brand_name),
    (month, global_entity_id, brand_name),

    -- 3. CATEGORY DEEP-DIVE (By Owner + Categories)
    (month, global_entity_id, principal_supplier_id, l1_master_category),
    (month, global_entity_id, principal_supplier_id, l2_master_category),
    (month, global_entity_id, principal_supplier_id, l3_master_category),

    (month, global_entity_id, supplier_id, l1_master_category),
    (month, global_entity_id, supplier_id, l2_master_category),
    (month, global_entity_id, supplier_id, l3_master_category),

    (month, global_entity_id, brand_owner_name, l1_master_category),
    (month, global_entity_id, brand_owner_name, l2_master_category),
    (month, global_entity_id, brand_owner_name, l3_master_category),

    (month, global_entity_id, brand_name, l1_master_category),
    (month, global_entity_id, brand_name, l2_master_category),
    (month, global_entity_id, brand_name, l3_master_category),

    -- ==========================================================
    -- QUARTERLY BREAKDOWNS (quarter_year)
    -- ==========================================================

    -- 1. TOTAL OWNER LEVEL
    (quarter_year, global_entity_id, principal_supplier_id),
    (quarter_year, global_entity_id, supplier_id),
    (quarter_year, global_entity_id, brand_owner_name),

    -- 2. BRAND DEEP-DIVE
    (quarter_year, global_entity_id, principal_supplier_id, brand_name),
    (quarter_year, global_entity_id, supplier_id, brand_name),
    (quarter_year, global_entity_id, brand_owner_name, brand_name),
    (quarter_year, global_entity_id, brand_name),

    -- 3. CATEGORY DEEP-DIVE
    (quarter_year, global_entity_id, principal_supplier_id, l1_master_category),
    (quarter_year, global_entity_id, principal_supplier_id, l2_master_category),
    (quarter_year, global_entity_id, principal_supplier_id, l3_master_category),

    (quarter_year, global_entity_id, supplier_id, l1_master_category),
    (quarter_year, global_entity_id, supplier_id, l2_master_category),
    (quarter_year, global_entity_id, supplier_id, l3_master_category),

    (quarter_year, global_entity_id, brand_owner_name, l1_master_category),
    (quarter_year, global_entity_id, brand_owner_name, l2_master_category),
    (quarter_year, global_entity_id, brand_owner_name, l3_master_category),

    (quarter_year, global_entity_id, brand_name, l1_master_category),
    (quarter_year, global_entity_id, brand_name, l2_master_category),
    (quarter_year, global_entity_id, brand_name, l3_master_category)
);