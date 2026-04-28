-- This table aggregates line rebate metrics for generating Supplier Scorecards.
-- SPS Execution: Position No. 4.2
-- Ingredientes añadidos en grouping sets:
--   SUM(sku_calc_gross_delivered) AS calc_gross_delivered
--   SUM(sku_calc_gross_return)    AS calc_gross_return
--   SUM(sku_calc_net_delivered)   AS calc_net_delivered
--   SUM(sku_calc_net_return)      AS calc_net_return
-- net_purchase y total_rebate se mantienen para compatibilidad

-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'PY_PE';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-01-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.ytd_sps_line_rebate_metrics`
CLUSTER BY global_entity_id, time_period
AS
WITH date_config AS (
  SELECT
    CURRENT_DATE() as today,
    EXTRACT(YEAR FROM CURRENT_DATE()) as current_year,
    EXTRACT(YEAR FROM CURRENT_DATE()) - 1 as prior_year
),
base AS (
  SELECT
    DATE(month) AS month_date,
    month,
    quarter_year,
    ytd_year,
    global_entity_id,
    supplier_id,
    principal_supplier_id,
    brand_name,
    brand_owner_name,
    front_facing_level_one,
    front_facing_level_two,
    l1_master_category,
    l2_master_category,
    l3_master_category,
    sku_calc_gross_delivered,
    sku_calc_gross_return,
    sku_calc_net_delivered,
    sku_calc_net_return,
    sku_rebate,
    sku_rebate_wo_dist_allowance_lc
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_line_rebate_metrics_month`
),
filtered AS (
  SELECT b.*
  FROM base b
  CROSS JOIN date_config d
  WHERE (EXTRACT(YEAR FROM b.month_date) = d.current_year AND b.month_date <= d.today)
    OR (EXTRACT(YEAR FROM b.month_date) = d.prior_year AND b.month_date <= DATE_SUB(d.today, INTERVAL 1 YEAR))
)
SELECT
  global_entity_id,
  CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING)
         WHEN GROUPING(quarter_year) = 0 THEN quarter_year
         ELSE CONCAT("YTD-", CAST(ytd_year AS STRING))
    END AS time_period,
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
      IF(GROUPING(front_facing_level_two) = 0, front_facing_level_two, NULL),
      IF(GROUPING(front_facing_level_one) = 0, front_facing_level_one, NULL),
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
        WHEN GROUPING(front_facing_level_two) = 0 THEN 'front_facing_level_two'
        WHEN GROUPING(front_facing_level_one) = 0 THEN 'front_facing_level_one'
        WHEN GROUPING(brand_name) = 0 THEN 'brand_name'
        ELSE 'supplier'
    END AS supplier_level,
  CASE WHEN GROUPING(month) = 0 THEN 'Monthly'
         WHEN GROUPING(quarter_year) = 0 THEN 'Quarterly'
         ELSE 'YTD'
    END AS time_granularity,
  ROUND(SUM(sku_calc_net_delivered - sku_calc_net_return), 2) AS net_purchase,
  ROUND(SUM(sku_rebate), 2) AS total_rebate,
  ROUND(SUM(sku_rebate_wo_dist_allowance_lc), 4) AS total_rebate_wo_dist_allowance_lc,
  -- Ingredientes añadidos
  ROUND(SUM(sku_calc_gross_delivered), 2) AS calc_gross_delivered,
  ROUND(SUM(sku_calc_gross_return), 2)    AS calc_gross_return,
  ROUND(SUM(sku_calc_net_delivered), 2)   AS calc_net_delivered,
  ROUND(SUM(sku_calc_net_return), 2)      AS calc_net_return,
  front_facing_level_one,
  front_facing_level_two
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

    -- 4. FRONT-FACING LEVEL DEEP-DIVE
    (month, global_entity_id, principal_supplier_id, front_facing_level_one),
    (month, global_entity_id, principal_supplier_id, front_facing_level_two),
    (month, global_entity_id, supplier_id, front_facing_level_one),
    (month, global_entity_id, supplier_id, front_facing_level_two),
    (month, global_entity_id, brand_owner_name, front_facing_level_one),
    (month, global_entity_id, brand_owner_name, front_facing_level_two),
    (month, global_entity_id, brand_name, front_facing_level_one),
    (month, global_entity_id, brand_name, front_facing_level_two),

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
    (quarter_year, global_entity_id, brand_name, l3_master_category),

    -- 4. FRONT-FACING LEVEL DEEP-DIVE
    (quarter_year, global_entity_id, principal_supplier_id, front_facing_level_one),
    (quarter_year, global_entity_id, principal_supplier_id, front_facing_level_two),
    (quarter_year, global_entity_id, supplier_id, front_facing_level_one),
    (quarter_year, global_entity_id, supplier_id, front_facing_level_two),
    (quarter_year, global_entity_id, brand_owner_name, front_facing_level_one),
    (quarter_year, global_entity_id, brand_owner_name, front_facing_level_two),
    (quarter_year, global_entity_id, brand_name, front_facing_level_one),
    (quarter_year, global_entity_id, brand_name, front_facing_level_two),

    -- ==========================================================
    -- YTD BREAKDOWNS (ytd_year)
    -- ==========================================================

    -- 1. TOTAL OWNER LEVEL
    (ytd_year, global_entity_id, principal_supplier_id),
    (ytd_year, global_entity_id, supplier_id),
    (ytd_year, global_entity_id, brand_owner_name),

    -- 2. BRAND DEEP-DIVE
    (ytd_year, global_entity_id, principal_supplier_id, brand_name),
    (ytd_year, global_entity_id, supplier_id, brand_name),
    (ytd_year, global_entity_id, brand_owner_name, brand_name),
    (ytd_year, global_entity_id, brand_name),

    -- 3. CATEGORY DEEP-DIVE
    (ytd_year, global_entity_id, principal_supplier_id, l1_master_category),
    (ytd_year, global_entity_id, principal_supplier_id, l2_master_category),
    (ytd_year, global_entity_id, principal_supplier_id, l3_master_category),

    (ytd_year, global_entity_id, supplier_id, l1_master_category),
    (ytd_year, global_entity_id, supplier_id, l2_master_category),
    (ytd_year, global_entity_id, supplier_id, l3_master_category),

    (ytd_year, global_entity_id, brand_owner_name, l1_master_category),
    (ytd_year, global_entity_id, brand_owner_name, l2_master_category),
    (ytd_year, global_entity_id, brand_owner_name, l3_master_category),

    (ytd_year, global_entity_id, brand_name, l1_master_category),
    (ytd_year, global_entity_id, brand_name, l2_master_category),
    (ytd_year, global_entity_id, brand_name, l3_master_category),

    -- 4. FRONT-FACING LEVEL DEEP-DIVE
    (ytd_year, global_entity_id, principal_supplier_id, front_facing_level_one),
    (ytd_year, global_entity_id, principal_supplier_id, front_facing_level_two),
    (ytd_year, global_entity_id, supplier_id, front_facing_level_one),
    (ytd_year, global_entity_id, supplier_id, front_facing_level_two),
    (ytd_year, global_entity_id, brand_owner_name, front_facing_level_one),
    (ytd_year, global_entity_id, brand_owner_name, front_facing_level_two),
    (ytd_year, global_entity_id, brand_name, front_facing_level_one),
    (ytd_year, global_entity_id, brand_name, front_facing_level_two)
);