-- Validation Queries for Front-Facing Categories Implementation

-- Query 1: Check ytd_sps_product - Uniqueness per (global_entity_id, sku, warehouse_id)
-- Should return 0 rows (each SKU should have exactly 1 front_facing value per warehouse)
SELECT
  'ytd_sps_product uniqueness check' AS check_name,
  COUNT(*) AS issue_count
FROM (
  SELECT
    global_entity_id,
    sku_id,
    warehouse_id,
    COUNT(DISTINCT front_facing_level_one) AS distinct_level_one,
    COUNT(DISTINCT front_facing_level_two) AS distinct_level_two
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_product`
  WHERE global_entity_id = 'PY_PE'
  GROUP BY global_entity_id, sku_id, warehouse_id
)
WHERE distinct_level_one > 1 OR distinct_level_two > 1;

-- Query 2: Check for NULL or '_unknown_' values in ytd_sps_product
SELECT
  'ytd_sps_product null check' AS check_name,
  COUNT(*) AS unknown_count
FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_product`
WHERE global_entity_id = 'PY_PE'
  AND (front_facing_level_one = '_unknown_' OR front_facing_level_two = '_unknown_');

-- Query 3: Check supplier_level distribution in ytd_sps_purchase_order
SELECT
  'ytd_sps_purchase_order supplier_level' AS check_name,
  supplier_level,
  COUNT(*) AS row_count
FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_purchase_order`
WHERE global_entity_id = 'PY_PE'
  AND time_granularity = 'Monthly'
GROUP BY supplier_level
ORDER BY row_count DESC;

-- Query 4: Check for front_facing data flow through ytd_sps_financial_metrics
SELECT
  'ytd_sps_financial_metrics sample' AS check_name,
  COUNT(*) AS row_count,
  COUNT(DISTINCT supplier_level) AS distinct_supplier_levels
FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_financial_metrics`
WHERE global_entity_id = 'PY_PE'
  AND supplier_level IN ('front_facing_level_one', 'front_facing_level_two');

-- Query 5: Verify front_facing fields are in ytd_sps_score_tableau
SELECT
  'ytd_sps_score_tableau sample' AS check_name,
  COUNT(*) AS row_count
FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_score_tableau`
WHERE global_entity_id = 'PY_PE'
  AND (front_facing_level_one != '_unknown_' OR front_facing_level_two != '_unknown_')
LIMIT 5;

-- Query 6: Compare ytd_sps_product field cardinality across all entities
SELECT
  'ytd_sps_product cardinality' AS check_name,
  COUNT(DISTINCT (global_entity_id, sku_id)) AS unique_sku_entities,
  COUNT(DISTINCT (global_entity_id, sku_id, front_facing_level_one)) AS with_ff_level_one,
  COUNT(DISTINCT (global_entity_id, sku_id, front_facing_level_two)) AS with_ff_level_two
FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_product`;
