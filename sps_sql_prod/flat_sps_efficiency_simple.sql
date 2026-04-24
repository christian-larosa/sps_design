-- ============================================================
-- SPS DEBUG | PY_PE | 08b sps_efficiency (SIMPLE VERSION)
-- ============================================================
-- Simplified approach: SKU classification ranking by warehouse count
-- Eliminates complex weight_efficiency calculation
-- Uses ROW_NUMBER() to pick predominant sku_efficiency class per SKU
-- ============================================================

-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'FP_HK|FP_PH|FP_SG|GV_ES|GV_IT|GV_UA|HF_EG|HS_SA|IN_AE|IN_EG|NP_HU|PY_AR|PY_CL|PY_PE|TB_AE|TB_BH|TB_JO|TB_KW|TB_OM|TB_QA|YS_TR';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-10-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_efficiency_simple`
CLUSTER BY
   global_entity_id,
   time_period
AS
WITH date_config AS (
  SELECT
     DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 4 QUARTER) AS lookback_limit
),

-- CTE A: Grain of (sku_id, sku_efficiency) — count how many warehouses per class
warehouse_sku_classification AS (
  SELECT
    sku_id,
    sku_efficiency,
    COUNT(DISTINCT warehouse_id) AS warehouse_count
  FROM `dh-darkstores-live.csm_automated_tables.sps_efficiency_month`
  WHERE CAST(month AS DATE) >= (SELECT lookback_limit FROM date_config)
  GROUP BY sku_id, sku_efficiency
),

-- CTE B: Pick predominant classification (highest warehouse count)
-- If tie, ORDER BY sku_efficiency alphabetically (consistent tiebreaker)
sku_predominant_class AS (
  SELECT
    sku_id,
    sku_efficiency,
    ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY warehouse_count DESC, sku_efficiency ASC) AS rank
  FROM warehouse_sku_classification
),

-- CTE C: Join main table with predominant class for accurate counting
sku_with_predominant_class AS (
  SELECT
    sps.*,
    COALESCE(pc.sku_efficiency, sps.sku_efficiency) AS predominant_sku_efficiency
  FROM `dh-darkstores-live.csm_automated_tables.sps_efficiency_month` sps
  LEFT JOIN sku_predominant_class pc
    ON sps.sku_id = pc.sku_id AND pc.rank = 1
  WHERE CAST(sps.month AS DATE) >= (SELECT lookback_limit FROM date_config)
),

-- CTE D: SKU counts using predominant classification
sku_counts AS (
  SELECT
    global_entity_id,
    month,
    quarter_year,
    supplier_id,
    principal_supplier_id,
    brand_name,
    brand_owner_name,
    l1_master_category,
    l2_master_category,
    l3_master_category,

    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      THEN sku_id END) AS sku_listed,

    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age >= 90 THEN sku_id END) AS sku_mature,

    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age < 90 AND updated_sku_age > 30
      THEN sku_id END) AS sku_probation,

    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age <= 30 THEN sku_id END) AS sku_new,

    -- Use predominant classification
    -- Mature SKUs (age >= 90) using predominant classification
    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age >= 90
      AND predominant_sku_efficiency = 'efficient_sku'
      THEN sku_id END) AS efficient_movers,

    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age >= 90
      AND predominant_sku_efficiency = 'slow_mover'
      AND ROUND(new_availability, 3) >= 0.8
      THEN sku_id END) AS slow_movers,

    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age >= 90
      AND predominant_sku_efficiency = 'zero_mover'
      AND ROUND(new_availability, 3) = 1.0
      THEN sku_id END) AS zero_movers,

    -- New SKUs (age < 30) using predominant classification
    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age < 30
      AND predominant_sku_efficiency = 'zero_mover'
      THEN sku_id END) AS new_zero_movers,

    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age < 30
      AND predominant_sku_efficiency = 'slow_mover'
      THEN sku_id END) AS new_slow_movers,

    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age < 90
      AND predominant_sku_efficiency = 'efficient_sku'
      THEN sku_id END) AS new_efficient_movers

  FROM sku_with_predominant_class
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),

-- CTE E: Additive metrics (GPV, sold items) — no window functions needed
financial_metrics AS (
  SELECT
    global_entity_id,
    month,
    quarter_year,
    supplier_id,
    principal_supplier_id,
    brand_name,
    brand_owner_name,
    l1_master_category,
    l2_master_category,
    l3_master_category,

    ROUND(SUM(sold_items), 1) AS sold_items,
    SUM(gpv_eur) AS gpv_eur

  FROM `dh-darkstores-live.csm_automated_tables.sps_efficiency_month`
  WHERE CAST(month AS DATE) >= (SELECT lookback_limit FROM date_config)
  GROUP BY
    global_entity_id,
    month,
    quarter_year,
    supplier_id,
    principal_supplier_id,
    brand_name,
    brand_owner_name,
    l1_master_category,
    l2_master_category,
    l3_master_category
),

-- CTE F: Combine SKU counts with financial metrics
combined AS (
  SELECT
    a.global_entity_id,
    a.month,
    a.quarter_year,
    a.supplier_id,
    a.principal_supplier_id,
    a.brand_name,
    a.brand_owner_name,
    a.l1_master_category,
    a.l2_master_category,
    a.l3_master_category,
    a.sku_listed,
    a.sku_mature,
    a.sku_probation,
    a.sku_new,
    a.efficient_movers,
    a.slow_movers,
    a.zero_movers,
    a.new_zero_movers,
    a.new_slow_movers,
    a.new_efficient_movers,
    b.gpv_eur,
    b.sold_items
  FROM sku_counts a
  LEFT JOIN financial_metrics b
    ON  a.global_entity_id   = b.global_entity_id
    AND a.month              = b.month
    AND a.supplier_id        = b.supplier_id
    AND a.brand_name         = b.brand_name
    AND a.brand_owner_name   = b.brand_owner_name
    AND a.l1_master_category = b.l1_master_category
    AND a.l2_master_category = b.l2_master_category
    AND a.l3_master_category = b.l3_master_category
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
    CASE
        WHEN GROUPING(month) = 0 THEN CAST(DATE_SUB(DATE(month), INTERVAL 1 YEAR) AS STRING)
        ELSE CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) - 1 AS STRING))
    END AS last_year_time_period,
    SUM(sku_listed) AS sku_listed,
    SUM(sku_mature) AS sku_mature,
    SUM(sku_probation) AS sku_probation,
    SUM(sku_new) AS sku_new,
    SUM(efficient_movers) AS efficient_movers,
    SUM(slow_movers) AS slow_movers,
    SUM(zero_movers) AS zero_movers,
    SUM(new_zero_movers) AS new_zero_movers,
    SUM(new_slow_movers) AS new_slow_movers,
    SUM(new_efficient_movers) AS new_efficient_movers,
    ROUND(SUM(sold_items),1) AS sold_items,
    ROUND(SUM(gpv_eur),1) AS gpv_eur
   FROM combined
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
