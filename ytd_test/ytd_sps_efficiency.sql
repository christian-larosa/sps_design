-- This table aggregates efficiency metrics for generating Supplier Scorecards.
-- SPS Execution: Position No. 7.2

-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'PY_PE';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-01-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.ytd_sps_efficiency`
CLUSTER BY
   global_entity_id,
   time_period
AS
WITH date_config AS (
  SELECT
    CURRENT_DATE() as today,
    EXTRACT(YEAR FROM CURRENT_DATE()) as current_year,
    EXTRACT(YEAR FROM CURRENT_DATE()) - 1 as prior_year
),

-- CTE A: conteos de SKUs únicos a nivel (supplier, categoría, mes) SIN warehouse_id
-- COUNT(DISTINCT) no es aditivo cross-warehouse → se calcula aquí una sola vez
sku_counts AS (
  SELECT
    global_entity_id,
    month,
    quarter_year,
    ytd_year,
    supplier_id,
    principal_supplier_id,
    brand_name,
    brand_owner_name,
    front_facing_level_one,
    front_facing_level_two,
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
    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age >= 90
      AND sku_efficiency = 'efficient_sku'
      THEN sku_id END) AS efficient_movers,
    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age < 30
      AND sku_efficiency = 'zero_mover'
      THEN sku_id END) AS new_zero_movers,
    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age < 30
      AND sku_efficiency = 'slow_mover'
      THEN sku_id END) AS new_slow_movers,
    COUNT(DISTINCT CASE WHEN is_listed = TRUE
      AND updated_sku_age < 90
      AND sku_efficiency = 'efficient_sku'
      THEN sku_id END) AS new_efficient_movers
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_efficiency_month`
  WHERE (EXTRACT(YEAR FROM CAST(month AS DATE)) = (SELECT current_year FROM date_config)
         AND CAST(month AS DATE) <= (SELECT today FROM date_config))
    OR (EXTRACT(YEAR FROM CAST(month AS DATE)) = (SELECT prior_year FROM date_config))
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
),

-- CTE B: métricas aditivas a nivel (supplier, warehouse, categoría, mes)
-- weight_efficiency y gpv_eur son aditivos cross-warehouse → SUM() es correcto
-- Sin COUNT(DISTINCT) de SKUs — esos viven en sku_counts
efficiency_by_warehouse AS (
  SELECT
    global_entity_id,
    month,
    quarter_year,
    ytd_year,
    supplier_id,
    principal_supplier_id,
    brand_name,
    brand_owner_name,
    front_facing_level_one,
    front_facing_level_two,
    l1_master_category,
    l2_master_category,
    l3_master_category,
    warehouse_id,

    SUM(numerator_new_avail) AS numerator_new_avail,
    SUM(denom_new_avail) AS denom_new_avail,
    ROUND(SUM(sold_items), 1) AS sold_items,
    SUM(gpv_eur) AS gpv_eur,

    ROUND(
      SAFE_DIVIDE(
        SUM(COUNT(DISTINCT CASE WHEN is_listed = TRUE
          AND updated_sku_age >= 90
          AND sku_efficiency = 'efficient_sku'
          THEN sku_id END))
          OVER (PARTITION BY global_entity_id, month,
                supplier_id, warehouse_id),
        NULLIF(SUM(COUNT(DISTINCT CASE WHEN is_listed = TRUE
          AND updated_sku_age >= 90
          AND (
            (sku_efficiency = 'efficient_sku')
            OR (sku_efficiency = 'slow_mover'
                AND ROUND(new_availability,3) >= 0.8)
            OR (sku_efficiency = 'zero_mover'
                AND ROUND(new_availability,3) = 1)
          ) THEN sku_id END))
          OVER (PARTITION BY global_entity_id, month,
                supplier_id, warehouse_id), 0)
      ) * SUM(gpv_eur)
    , 4) AS weight_efficiency

  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_efficiency_month`
  WHERE (EXTRACT(YEAR FROM CAST(month AS DATE)) = (SELECT current_year FROM date_config)
         AND CAST(month AS DATE) <= (SELECT today FROM date_config))
    OR (EXTRACT(YEAR FROM CAST(month AS DATE)) = (SELECT prior_year FROM date_config))
  GROUP BY
    global_entity_id,
    month,
    quarter_year,
    ytd_year,
    supplier_id,
    principal_supplier_id,
    brand_name,
    brand_owner_name,
    front_facing_level_one,
    front_facing_level_two,
    l1_master_category,
    l2_master_category,
    l3_master_category,
    warehouse_id
),

-- CTE C: combina conteos de SKUs (sin warehouse) con métricas aditivas (con warehouse)
-- Los conteos de SKU quedan fijos del CTE A; gpv/efficiency se suman cross-warehouse
combined AS (
  SELECT
    a.global_entity_id,
    a.month,
    a.quarter_year,
    a.ytd_year,
    a.supplier_id,
    a.principal_supplier_id,
    a.brand_name,
    a.brand_owner_name,
    a.front_facing_level_one,
    a.front_facing_level_two,
    a.l1_master_category,
    a.l2_master_category,
    a.l3_master_category,
    a.sku_listed,
    a.sku_mature,
    a.sku_probation,
    a.sku_new,
    a.efficient_movers,
    a.new_zero_movers,
    a.new_slow_movers,
    a.new_efficient_movers,
    SUM(b.gpv_eur)              AS gpv_eur,
    SUM(b.numerator_new_avail)  AS numerator_new_avail,
    SUM(b.denom_new_avail)      AS denom_new_avail,
    ROUND(SUM(b.sold_items), 1) AS sold_items,
    SUM(b.weight_efficiency)    AS weight_efficiency
  FROM sku_counts a
  LEFT JOIN efficiency_by_warehouse b
    ON  a.global_entity_id   = b.global_entity_id
    AND a.month              = b.month
    AND a.quarter_year       = b.quarter_year
    AND a.ytd_year           = b.ytd_year
    AND a.supplier_id        = b.supplier_id
    AND a.brand_name         = b.brand_name
    AND a.brand_owner_name   = b.brand_owner_name
    AND a.front_facing_level_one = b.front_facing_level_one
    AND a.front_facing_level_two = b.front_facing_level_two
    AND a.l1_master_category = b.l1_master_category
    AND a.l2_master_category = b.l2_master_category
    AND a.l3_master_category = b.l3_master_category
  GROUP BY
    a.global_entity_id, a.month, a.quarter_year, a.ytd_year,
    a.supplier_id, a.principal_supplier_id,
    a.brand_name, a.brand_owner_name,
    a.front_facing_level_one, a.front_facing_level_two,
    a.l1_master_category, a.l2_master_category, a.l3_master_category,
    a.sku_listed, a.sku_mature, a.sku_probation, a.sku_new,
    a.efficient_movers, a.new_zero_movers, a.new_slow_movers,
    a.new_efficient_movers
),
monthly_quarterly_data AS (
  SELECT
    global_entity_id,
    CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING)
         WHEN GROUPING(quarter_year) = 0 THEN quarter_year
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
    END AS time_granularity,
    SUM(sku_listed) AS sku_listed,
    SUM(sku_mature) AS sku_mature,
    SUM(sku_probation) AS sku_probation,
    SUM(sku_new) AS sku_new,
    SUM(efficient_movers) AS efficient_movers,
    SUM(new_zero_movers) AS new_zero_movers,
    SUM(new_slow_movers) AS new_slow_movers,
    SUM(new_efficient_movers) AS new_efficient_movers,
    ROUND(SUM(sold_items),1) AS sold_items,
    ROUND(SUM(gpv_eur),1) AS gpv_eur,
    SUM(numerator_new_avail) AS numerator_new_avail,
    SUM(denom_new_avail) AS denom_new_avail,
    SUM(weight_efficiency) AS weight_efficiency
  FROM combined
  GROUP BY GROUPING SETS (
      (month, global_entity_id, principal_supplier_id),
      (month, global_entity_id, supplier_id),
      (month, global_entity_id, brand_owner_name),
      (month, global_entity_id, principal_supplier_id, brand_name),
      (month, global_entity_id, supplier_id, brand_name),
      (month, global_entity_id, brand_owner_name, brand_name),
      (month, global_entity_id, brand_name),
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
      (month, global_entity_id, principal_supplier_id, front_facing_level_one),
      (month, global_entity_id, principal_supplier_id, front_facing_level_two),
      (month, global_entity_id, supplier_id, front_facing_level_one),
      (month, global_entity_id, supplier_id, front_facing_level_two),
      (month, global_entity_id, brand_owner_name, front_facing_level_one),
      (month, global_entity_id, brand_owner_name, front_facing_level_two),
      (month, global_entity_id, brand_name, front_facing_level_one),
      (month, global_entity_id, brand_name, front_facing_level_two),
      (quarter_year, global_entity_id, principal_supplier_id),
      (quarter_year, global_entity_id, supplier_id),
      (quarter_year, global_entity_id, brand_owner_name),
      (quarter_year, global_entity_id, principal_supplier_id, brand_name),
      (quarter_year, global_entity_id, supplier_id, brand_name),
      (quarter_year, global_entity_id, brand_owner_name, brand_name),
      (quarter_year, global_entity_id, brand_name),
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
      (quarter_year, global_entity_id, principal_supplier_id, front_facing_level_one),
      (quarter_year, global_entity_id, principal_supplier_id, front_facing_level_two),
      (quarter_year, global_entity_id, supplier_id, front_facing_level_one),
      (quarter_year, global_entity_id, supplier_id, front_facing_level_two),
      (quarter_year, global_entity_id, brand_owner_name, front_facing_level_one),
      (quarter_year, global_entity_id, brand_owner_name, front_facing_level_two),
      (quarter_year, global_entity_id, brand_name, front_facing_level_one),
      (quarter_year, global_entity_id, brand_name, front_facing_level_two)
  )
),
ytd_data AS (
  SELECT
    global_entity_id,
    CONCAT('YTD-', CAST(ytd_year AS STRING)) AS time_period,
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
    'YTD' AS time_granularity,
    SUM(sku_listed) AS sku_listed,
    SUM(sku_mature) AS sku_mature,
    SUM(sku_probation) AS sku_probation,
    SUM(sku_new) AS sku_new,
    SUM(efficient_movers) AS efficient_movers,
    SUM(new_zero_movers) AS new_zero_movers,
    SUM(new_slow_movers) AS new_slow_movers,
    SUM(new_efficient_movers) AS new_efficient_movers,
    ROUND(SUM(sold_items),1) AS sold_items,
    ROUND(SUM(gpv_eur),1) AS gpv_eur,
    SUM(numerator_new_avail) AS numerator_new_avail,
    SUM(denom_new_avail) AS denom_new_avail,
    SUM(weight_efficiency) AS weight_efficiency
  FROM combined
  GROUP BY GROUPING SETS (
      (ytd_year, global_entity_id, principal_supplier_id),
      (ytd_year, global_entity_id, supplier_id),
      (ytd_year, global_entity_id, brand_owner_name),
      (ytd_year, global_entity_id, principal_supplier_id, brand_name),
      (ytd_year, global_entity_id, supplier_id, brand_name),
      (ytd_year, global_entity_id, brand_owner_name, brand_name),
      (ytd_year, global_entity_id, brand_name),
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
      (ytd_year, global_entity_id, principal_supplier_id, front_facing_level_one),
      (ytd_year, global_entity_id, principal_supplier_id, front_facing_level_two),
      (ytd_year, global_entity_id, supplier_id, front_facing_level_one),
      (ytd_year, global_entity_id, supplier_id, front_facing_level_two),
      (ytd_year, global_entity_id, brand_owner_name, front_facing_level_one),
      (ytd_year, global_entity_id, brand_owner_name, front_facing_level_two),
      (ytd_year, global_entity_id, brand_name, front_facing_level_one),
      (ytd_year, global_entity_id, brand_name, front_facing_level_two)
  )
)

SELECT * FROM monthly_quarterly_data
UNION ALL
SELECT * FROM ytd_data;
