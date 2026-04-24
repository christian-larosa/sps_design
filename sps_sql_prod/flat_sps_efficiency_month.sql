-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'FP_HK|FP_PH|FP_SG|GV_ES|GV_IT|GV_UA|HF_EG|HS_SA|IN_AE|IN_EG|NP_HU|PY_AR|PY_CL|PY_PE|TB_AE|TB_BH|TB_JO|TB_KW|TB_OM|TB_QA|YS_TR';
DECLARE param_country_code STRING DEFAULT r'hk|ph|sg|es|it|ua|eg|sa|ae|hu|ar|cl|pe|bh|jo|kw|om|qa|tr';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-10-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

-- This table extracts and maintains the efficiency metrics mapping required for generating Supplier Scorecards.
-- SPS Execution: Position No. 7.1
-- DML SCRIPT: SPS Refact Incremental Refresh for dh-darkstores-live.csm_automated_tables.sps_efficiency_month
CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_efficiency_month`
AS
WITH
date_in AS (
  SELECT param_date_start AS date_in
),
date_fin AS (
  SELECT param_date_end AS date_fin
),
tmp_sp_product AS (
 SELECT
   sp.global_entity_id,
   sp.country_code,
   sp.sku_id,
   COALESCE(CAST(sp.supplier_id AS STRING), '_unknown_') AS supplier_id,
   sp.supplier_name,
   sp.warehouse_id,
   COALESCE(sp.brand_name, '_unknown_') AS brand_name,
   COALESCE(sp.brand_owner_name, '_unknown_') AS brand_owner_name,
  --  sp.region_name,
   COALESCE(sp.level_one, '_unknown_') AS l1_master_category,
   COALESCE(sp.level_two, '_unknown_') AS l2_master_category,
   COALESCE(sp.level_three, '_unknown_') AS l3_master_category,
  --  COALESCE(CAST(ANY_VALUE(sp.sup_id_parent) AS STRING), '_unknown_') AS principal_supplier_id,
   ANY_VALUE(sp.sup_id_parent) AS principal_supplier_id,
   MAX(sp.updated_at) AS last_updated,
 FROM `dh-darkstores-live.csm_automated_tables.sps_product` AS sp
 WHERE TRUE
 AND REGEXP_CONTAINS(sp.global_entity_id, param_global_entity_id)
 AND REGEXP_CONTAINS(sp.country_code, param_country_code)
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),
ranked_global_product AS (
    -- This identifies the "latest" info for a SKU regardless of warehouse.
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY global_entity_id, sku_id ORDER BY last_updated DESC) as recency_rank
  FROM tmp_sp_product
),
tmp_efficiency AS (
   SELECT
   e.global_entity_id,
   e.warehouse_id,
   e.sku AS sku_id,
   e.updated_sku_age,
   e.sku_efficiency,
   e.avg_qty_sold,
   e.new_availability,
   e.numerator_new_avail,
   e.denom_new_avail,
   e.available_hours,
   e.potential_hours,
   e.is_listed,
   e.sku_status,
   e.sold_items,
   e.gpv_eur,
   STRING(e.month) AS month,
   CAST(CONCAT('Q', EXTRACT(QUARTER FROM e.month), '-', EXTRACT(YEAR FROM e.month)) AS STRING) AS quarter_year,
 FROM `fulfillment-dwh-production.cl_dmart.sku_efficiency_detail_v2` AS e
 WHERE TRUE
    AND (DATE(e.partition_month) BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
    AND REGEXP_CONTAINS(e.global_entity_id, param_global_entity_id)
)
SELECT
  CASE
    WHEN DATE_TRUNC(CAST(te.month AS DATE), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
    THEN CURRENT_DATE()
    ELSE LAST_DAY(CAST(te.month AS DATE))
  END AS partition_month,
  te.month,
  te.quarter_year,
  te.global_entity_id,
  te.warehouse_id,
  te.sku_id,
  te.updated_sku_age,
  te.sku_efficiency,
  te.is_listed,
  te.sku_status,
  te.avg_qty_sold,
  te.numerator_new_avail,
  te.denom_new_avail,
  te.available_hours,
  te.potential_hours,
  te.new_availability,
  te.sold_items,
  te.gpv_eur,
  COALESCE(sp_exact.country_code, sp_fallback.country_code) AS country_code,
  COALESCE(sp_exact.supplier_id, sp_fallback.supplier_id) AS supplier_id,
  COALESCE(sp_exact.supplier_name, sp_fallback.supplier_name) AS supplier_name,
  COALESCE(sp_exact.brand_name, sp_fallback.brand_name) AS brand_name,
  COALESCE(sp_exact.brand_owner_name, sp_fallback.brand_owner_name) AS brand_owner_name,
  COALESCE(sp_exact.l1_master_category, sp_fallback.l1_master_category) AS l1_master_category,
  COALESCE(sp_exact.l2_master_category, sp_fallback.l2_master_category) AS l2_master_category,
  COALESCE(sp_exact.l3_master_category, sp_fallback.l3_master_category) AS l3_master_category,
  COALESCE(sp_exact.principal_supplier_id, sp_fallback.principal_supplier_id) AS principal_supplier_id
FROM tmp_efficiency AS te
-- Join exacto por warehouse (mismo que el original)
LEFT JOIN tmp_sp_product AS sp_exact
  ON te.sku_id = sp_exact.sku_id
  AND te.global_entity_id = sp_exact.global_entity_id
  AND te.warehouse_id = sp_exact.warehouse_id
-- Fallback: SKU más reciente en cualquier warehouse del país
LEFT JOIN ranked_global_product AS sp_fallback
  ON te.sku_id = sp_fallback.sku_id
  AND te.global_entity_id = sp_fallback.global_entity_id
  AND sp_fallback.recency_rank = 1
  AND sp_exact.sku_id IS NULL
WHERE TRUE;

