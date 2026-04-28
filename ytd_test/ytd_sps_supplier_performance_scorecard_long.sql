CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.ytd_sps_supplier_radar_long` AS

WITH base AS (
  SELECT
    global_entity_id,
    time_period,
    time_granularity,
    division_type,
    supplier_level,
    entity_key,
    supplier_name,
    brand_sup,
    segment_lc,

    SAFE_DIVIDE(SUM(wscore_num_yoy), SUM(wscore_denom)) / 10 * 100 AS radar_growth_yoy,
    SAFE_DIVIDE(SUM(wscore_num_efficiency), SUM(wscore_denom)) / 30 * 100 AS radar_efficiency,
    SAFE_DIVIDE(SUM(wscore_num_gbd), SUM(wscore_denom)) / 20 * 100 AS radar_gbd,
    SAFE_DIVIDE(SUM(wscore_num_back_margin), SUM(wscore_denom)) / 25 * 100 AS radar_back_margin,
    SAFE_DIVIDE(SUM(wscore_num_front_margin), SUM(wscore_denom)) / 15 * 100 AS radar_front_margin
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_supplier_master`
  GROUP BY
    global_entity_id,
    time_period,
    time_granularity,
    division_type,
    supplier_level,
    entity_key,
    supplier_name,
    brand_sup,
    segment_lc
),

global_benchmark AS (
  SELECT
    'Global' AS benchmark_scope,
    'Global' AS benchmark_label,
    time_period,
    time_granularity,
    division_type,
    supplier_level,
    CAST(NULL AS STRING) AS global_entity_id,
    CAST(NULL AS STRING) AS segment_lc,
    CAST(NULL AS STRING) AS entity_key,
    CAST(NULL AS STRING) AS supplier_name,
    CAST(NULL AS STRING) AS brand_sup,

    SAFE_DIVIDE(SUM(wscore_num_yoy), SUM(wscore_denom)) / 10 * 100 AS radar_growth_yoy,
    SAFE_DIVIDE(SUM(wscore_num_efficiency), SUM(wscore_denom)) / 30 * 100 AS radar_efficiency,
    SAFE_DIVIDE(SUM(wscore_num_gbd), SUM(wscore_denom)) / 20 * 100 AS radar_gbd,
    SAFE_DIVIDE(SUM(wscore_num_back_margin), SUM(wscore_denom)) / 25 * 100 AS radar_back_margin,
    SAFE_DIVIDE(SUM(wscore_num_front_margin), SUM(wscore_denom)) / 15 * 100 AS radar_front_margin
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_supplier_master`
  GROUP BY
    time_period,
    time_granularity,
    division_type,
    supplier_level
),

entity_benchmark AS (
  SELECT
    'Entity' AS benchmark_scope,
    global_entity_id AS benchmark_label,
    time_period,
    time_granularity,
    division_type,
    supplier_level,
    global_entity_id,
    CAST(NULL AS STRING) AS segment_lc,
    CAST(NULL AS STRING) AS entity_key,
    CAST(NULL AS STRING) AS supplier_name,
    CAST(NULL AS STRING) AS brand_sup,

    SAFE_DIVIDE(SUM(wscore_num_yoy), SUM(wscore_denom)) / 10 * 100 AS radar_growth_yoy,
    SAFE_DIVIDE(SUM(wscore_num_efficiency), SUM(wscore_denom)) / 30 * 100 AS radar_efficiency,
    SAFE_DIVIDE(SUM(wscore_num_gbd), SUM(wscore_denom)) / 20 * 100 AS radar_gbd,
    SAFE_DIVIDE(SUM(wscore_num_back_margin), SUM(wscore_denom)) / 25 * 100 AS radar_back_margin,
    SAFE_DIVIDE(SUM(wscore_num_front_margin), SUM(wscore_denom)) / 15 * 100 AS radar_front_margin
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_supplier_master`
  GROUP BY
    global_entity_id,
    time_period,
    time_granularity,
    division_type,
    supplier_level
),

segment_benchmark AS (
  SELECT
    'Segment' AS benchmark_scope,
    segment_lc AS benchmark_label,
    time_period,
    time_granularity,
    division_type,
    supplier_level,
    global_entity_id,
    segment_lc,
    CAST(NULL AS STRING) AS entity_key,
    CAST(NULL AS STRING) AS supplier_name,
    CAST(NULL AS STRING) AS brand_sup,

    SAFE_DIVIDE(SUM(wscore_num_yoy), SUM(wscore_denom)) / 10 * 100 AS radar_growth_yoy,
    SAFE_DIVIDE(SUM(wscore_num_efficiency), SUM(wscore_denom)) / 30 * 100 AS radar_efficiency,
    SAFE_DIVIDE(SUM(wscore_num_gbd), SUM(wscore_denom)) / 20 * 100 AS radar_gbd,
    SAFE_DIVIDE(SUM(wscore_num_back_margin), SUM(wscore_denom)) / 25 * 100 AS radar_back_margin,
    SAFE_DIVIDE(SUM(wscore_num_front_margin), SUM(wscore_denom)) / 15 * 100 AS radar_front_margin
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_supplier_master`
  GROUP BY
    global_entity_id,
    segment_lc,
    time_period,
    time_granularity,
    division_type,
    supplier_level
),

supplier_benchmark AS (
  SELECT
    'Supplier' AS benchmark_scope,
    supplier_name AS benchmark_label,
    time_period,
    time_granularity,
    division_type,
    supplier_level,
    global_entity_id,
    segment_lc,
    entity_key,
    supplier_name,
    brand_sup,

    SAFE_DIVIDE(SUM(wscore_num_yoy), SUM(wscore_denom)) / 10 * 100 AS radar_growth_yoy,
    SAFE_DIVIDE(SUM(wscore_num_efficiency), SUM(wscore_denom)) / 30 * 100 AS radar_efficiency,
    SAFE_DIVIDE(SUM(wscore_num_gbd), SUM(wscore_denom)) / 20 * 100 AS radar_gbd,
    SAFE_DIVIDE(SUM(wscore_num_back_margin), SUM(wscore_denom)) / 25 * 100 AS radar_back_margin,
    SAFE_DIVIDE(SUM(wscore_num_front_margin), SUM(wscore_denom)) / 15 * 100 AS radar_front_margin
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_supplier_master`
  GROUP BY
    global_entity_id,
    segment_lc,
    entity_key,
    supplier_name,
    brand_sup,
    time_period,
    time_granularity,
    division_type,
    supplier_level
),

wide_union AS (
  SELECT * FROM global_benchmark
  UNION ALL
  SELECT * FROM entity_benchmark
  UNION ALL
  SELECT * FROM segment_benchmark
  UNION ALL
  SELECT * FROM supplier_benchmark
)

SELECT
  benchmark_scope,
  benchmark_label,
  time_period,
  time_granularity,
  division_type,
  supplier_level,
  global_entity_id,
  segment_lc,
  entity_key,
  supplier_name,
  brand_sup,
  metric_name,
  metric_value
FROM wide_union
UNPIVOT (
  metric_value FOR metric_name IN (
    radar_growth_yoy AS 'Growth YoY',
    radar_efficiency AS 'Efficiency',
    radar_gbd AS 'Gross Basket Discount',
    radar_back_margin AS 'Back Margin',
    radar_front_margin AS 'Front Margin'
  )
);