-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'FP_HK|FP_PH|FP_SG|GV_ES|GV_IT|GV_UA|HF_EG|HS_SA|IN_AE|IN_EG|NP_HU|PY_AR|PY_CL|PY_PE|TB_AE|TB_BH|TB_JO|TB_KW|TB_OM|TB_QA|YS_TR';
DECLARE param_country_code STRING DEFAULT r'hk|ph|sg|es|it|ua|eg|sa|ae|hu|ar|cl|pe|bh|jo|kw|om|qa|tr';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-10-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

-- This table extracts and maintains the shrinkage metrics mapping required for generating Supplier Scorecards.
-- SPS Execution: Position No. 11.1
-- DML SCRIPT: SPS Refact Incremental Refresh for dh-darkstores-live.csm_automated_tables.sps_shrinkage_month
CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_shrinkage_month`
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
    sp.sku_id,
    COALESCE(MAX(CAST(sp.supplier_id AS STRING)), '_unknown_') AS supplier_id,
    MAX(sp.sup_id_parent)                                      AS principal_supplier_id,
    COALESCE(MAX(sp.brand_name), '_unknown_')                  AS brand_name,
    COALESCE(MAX(sp.brand_owner_name), '_unknown_')            AS brand_owner_name,
    MAX(sp.global_supplier_id)                                 AS global_supplier_id,
    COALESCE(MAX(sp.level_one), '_unknown_')                   AS l1_master_category,
    COALESCE(MAX(sp.level_two), '_unknown_')                   AS l2_master_category,
    COALESCE(MAX(sp.level_three), '_unknown_')                 AS l3_master_category
  FROM `dh-darkstores-live.csm_automated_tables.sps_product` AS sp
  WHERE TRUE
    AND REGEXP_CONTAINS(sp.global_entity_id, param_global_entity_id)
    AND REGEXP_CONTAINS(sp.country_code, param_country_code)
  GROUP BY
    sp.global_entity_id,
    sp.sku_id
),
tmp_shrinkage AS (
    SELECT 
      global_entity_id,
      -- country_code,
      sku AS sku_id,
      CAST(supplier_id AS STRING ) AS supplier_id, 
      level_one_category AS l1_master_category,
      level_two_category AS l2_master_category,
      level_three_category AS l3_master_category,
      DATE_TRUNC(period, month) AS month,
      CAST(CONCAT('Q', EXTRACT(QUARTER FROM period), '-', EXTRACT(YEAR FROM period)) AS STRING) AS quarter_year,
      SUM(CASE WHEN full_stock_move_reason_raw = 'Store Expired' THEN movement_qty * wac_eod_eur ELSE 0 END) AS spoilage_value_eur,
      SUM(CASE WHEN full_stock_move_reason_raw = 'Store Expired' THEN movement_qty * wac_eod_lc  ELSE 0 END) AS spoilage_value_lc,
      SUM(retail_revenue_eur * is_sales_considered) AS retail_revenue_eur,
      SUM(retail_revenue_lc  * is_sales_considered) AS retail_revenue_lc,
    --   SUM(CASE WHEN full_stock_move_reason_raw = 'Store Expired' THEN movement_qty*wac_eod_eur ELSE 0 END)/
    -- NULLIF(SUM(retail_revenue_eur*is_sales_considered),0) AS spoilage_percent,
    from `fulfillment-dwh-production.rl_dmart.shrinkage_report`
    WHERE TRUE
    AND REGEXP_CONTAINS(global_entity_id, param_global_entity_id)
    AND is_dmart
     AND (DATE_TRUNC(period, MONTH) BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
    GROUP BY 1,2,3,4,5,6,7,8
    order by 1,2
)
SELECT 
  te_sh.global_entity_id,
  -- COALESCE(te_sh.country_code, sp_exact.country_code) AS country_code,
  te_sh.sku_id,
  te_sh.month,
  te_sh.quarter_year,
  te_sh.spoilage_value_eur,
  te_sh.spoilage_value_lc,
  te_sh.retail_revenue_eur,
  te_sh.retail_revenue_lc,
  COALESCE(te_sh.supplier_id, sp_exact.supplier_id) AS supplier_id,
  -- sp_exact.supplier_name,
  sp_exact.brand_name,
  sp_exact.brand_owner_name,
  COALESCE(te_sh.l1_master_category, sp_exact.l1_master_category) AS l1_master_category,
  COALESCE(te_sh.l2_master_category, sp_exact.l2_master_category) AS l2_master_category,
  COALESCE(te_sh.l3_master_category, sp_exact.l3_master_category) AS l3_master_category,
  sp_exact.principal_supplier_id,
  CASE
    WHEN DATE_TRUNC(CAST(te_sh.month AS DATE), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
    THEN CURRENT_DATE()
      ELSE LAST_DAY(CAST(te_sh.month AS DATE))
  END AS partition_month,
FROM tmp_shrinkage AS te_sh
JOIN tmp_sp_product AS sp_exact
  ON te_sh.sku_id = sp_exact.sku_id
  AND te_sh.global_entity_id = sp_exact.global_entity_id;
