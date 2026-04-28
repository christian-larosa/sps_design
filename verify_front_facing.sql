-- Verificar que front_facing_level_one y front_facing_level_two son únicos por (global_entity_id, sku)
WITH check_distinctness AS (
  SELECT
    global_entity_id,
    sku,
    COUNT(DISTINCT front_facing_level_one) AS distinct_level_one,
    COUNT(DISTINCT front_facing_level_two) AS distinct_level_two,
    COUNT(DISTINCT warehouse_id) AS distinct_warehouses,
    ARRAY_AGG(DISTINCT front_facing_level_one) AS all_level_one_values,
    ARRAY_AGG(DISTINCT front_facing_level_two) AS all_level_two_values
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_product`
  WHERE global_entity_id = 'PY_PE'
  GROUP BY global_entity_id, sku
)
SELECT
  *
FROM check_distinctness
WHERE distinct_level_one > 1 OR distinct_level_two > 1
LIMIT 20;
