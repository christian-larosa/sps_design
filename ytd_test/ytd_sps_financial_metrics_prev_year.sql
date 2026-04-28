-- CRITICAL FILE: YTD Previous Year with max_date_per_entity capping (Pattern F)
-- This ensures LY YTD window matches CY YTD window per entity
-- SPS Execution: Position No. 5.2 (YTD variant)

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.ytd_sps_financial_metrics_prev_year`
CLUSTER BY
   global_entity_id, 
   join_time_period
AS

WITH
-- Step 1: Find max_date per entity in current year (Pattern F anchor)
max_date_cy AS (
  SELECT
    global_entity_id,
    MAX(CAST(month AS DATE)) AS max_month_cy
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_financial_metrics_month`
  WHERE EXTRACT(YEAR FROM CAST(month AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE())
  GROUP BY global_entity_id
),

-- Step 2: Filter LY data by per-entity max_date cap (Pattern F)
filtered_ly AS (
  SELECT m.*
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_financial_metrics_month` m
  JOIN max_date_cy mx ON m.global_entity_id = mx.global_entity_id
  WHERE
    EXTRACT(YEAR FROM CAST(m.month AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
    AND CAST(m.month AS DATE) <= DATE_SUB(LAST_DAY(mx.max_month_cy), INTERVAL 1 YEAR)
),

-- Step 3: Aggregate with grouping sets
aggregated AS (
  SELECT
    global_entity_id,
    -- Shift time_period forward by 1 year for join key
    CASE 
      WHEN GROUPING(month) = 0 THEN CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING)
      WHEN GROUPING(quarter_year) = 0 THEN CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING))
      WHEN GROUPING(ytd_year) = 0 THEN CONCAT('YTD-', CAST(CAST(ytd_year AS INT64) + 1 AS STRING))
    END AS join_time_period,
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
    CASE 
      WHEN GROUPING(month) = 0 THEN 'Monthly'
      WHEN GROUPING(quarter_year) = 0 THEN 'Quarterly'
      WHEN GROUPING(ytd_year) = 0 THEN 'YTD'
    END AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC) AS Net_Sales_eur_LY,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC) AS Net_Sales_lc_LY
  FROM filtered_ly
  GROUP BY GROUPING SETS (
    -- MONTHLY BREAKDOWNS
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
    
    -- QUARTERLY BREAKDOWNS
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
    
    -- YTD BREAKDOWNS (Pattern B replica with ytd_year)
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
    (ytd_year, global_entity_id, brand_name, l3_master_category)
  )
)

SELECT * FROM aggregated
