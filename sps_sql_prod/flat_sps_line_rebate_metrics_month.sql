-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'FP_HK|FP_PH|FP_SG|GV_ES|GV_IT|GV_UA|HF_EG|HS_SA|IN_AE|IN_EG|NP_HU|PY_AR|PY_CL|PY_PE|TB_AE|TB_BH|TB_JO|TB_KW|TB_OM|TB_QA|YS_TR';
DECLARE param_country_code STRING DEFAULT r'hk|ph|sg|es|it|ua|eg|sa|ae|hu|ar|cl|pe|bh|jo|kw|om|qa|tr';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-10-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

-- This table extracts and maintains the line rebate mapping required for generating Supplier Scorecards.
-- SPS Execution: Position No. 4.1
-- DML SCRIPT: SPS Refact Incremental Refresh for dh-darkstores-live.csm_automated_tables.sps_line_rebate_metrics_month
CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_line_rebate_metrics_month`
AS
WITH
date_in AS (
  SELECT param_date_start AS date_in
),
date_fin AS (
  SELECT param_date_end AS date_fin
),
sps_product AS (
    SELECT
      sp.global_entity_id,
      sp.sku_id,
      COALESCE(CAST(sp.supplier_id AS STRING), '_unknown_') AS supplier_id,
      -- CAST(sp.supplier_id AS STRING) AS supplier_id,
      sp.sup_id_parent AS principal_supplier_id,
      CASE WHEN CAST(sp.supplier_id AS STRING) = sp.sup_id_parent THEN TRUE END AS is_sup_id_parent,
      COALESCE( NULLIF(LOWER(sp.brand_name), 'unbranded'), '_unknown_' ) AS brand_name,
      COALESCE( NULLIF(LOWER(sp.brand_owner_name), 'unbranded'), '_unknown_' ) AS brand_owner_name,
      sp.global_supplier_id,
      COALESCE(sp.level_one, '_unknown_') AS l1_master_category,
      COALESCE(sp.level_two, '_unknown_') AS l2_master_category,
      COALESCE(sp.level_three, '_unknown_') AS l3_master_category,
    FROM `dh-darkstores-live.csm_automated_tables.sps_product` AS sp
    WHERE TRUE
      AND REGEXP_CONTAINS(sp.country_code, param_country_code)
    GROUP BY ALL
  ),
  sps_line_rebate AS (
    SELECT
      lr.global_entity_id,
      lr.country_code,
      lr.sku,
      lr.sup_id,
      CAST(DATE_TRUNC(lr.month, MONTH) AS STRING) AS month,
      CAST(CONCAT('Q', EXTRACT(QUARTER FROM lr.month), '-', EXTRACT(YEAR FROM lr.month)) AS STRING) AS quarter_year,
      -- Ingredientes añadidos
      ROUND(IFNULL(SUM(lr.calc_gross_delivered), 0), 2) AS sku_calc_gross_delivered,
      ROUND(IFNULL(SUM(lr.calc_gross_return), 0), 2) AS sku_calc_gross_return,
      ROUND(IFNULL(SUM(lr.calc_net_delivered), 0), 2) AS sku_calc_net_delivered,
      ROUND(IFNULL(SUM(lr.calc_net_return), 0), 2) AS sku_calc_net_return,
      ROUND(IFNULL(SUM(lr.rebate), 0), 2) AS sku_rebate,
      ROUND(IFNULL(SUM(CASE WHEN lr.trading_term_name != 'Distribution Allowance' THEN lr.rebate ELSE 0 END), 0), 4) AS sku_rebate_wo_dist_allowance_lc
    FROM `fulfillment-dwh-production.cl_dmart.rb_line_rebate` AS lr
    WHERE TRUE
      AND lr.trading_term_type NOT IN ('Frontmargin')
      AND (DATE_TRUNC(DATE(lr.month), MONTH) BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
      AND REGEXP_CONTAINS(lr.country_code, param_country_code)
    GROUP BY 1, 2, 3, 4, 5, 6
  )
    SELECT
      lr.global_entity_id,
      lr.country_code,
      lr.sku,
      lr.sup_id AS supplier_id,
      lr.month,
      lr.quarter_year,
      lr.sku_calc_gross_delivered,
      lr.sku_calc_gross_return,
      lr.sku_calc_net_delivered,
      lr.sku_calc_net_return,
      lr.sku_rebate,
      lr.sku_rebate_wo_dist_allowance_lc,
      so.principal_supplier_id,
      so.brand_name,
      so.brand_owner_name,
      so.l1_master_category,
      so.l2_master_category,
      so.l3_master_category,
      CASE
        WHEN DATE_TRUNC(CAST(lr.month AS DATE), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
        THEN CURRENT_DATE()
          ELSE LAST_DAY(CAST(lr.month AS DATE))
      END AS partition_month,
    FROM sps_line_rebate AS lr
    LEFT JOIN sps_product AS so
      ON lr.global_entity_id = so.global_entity_id
      AND lr.sku = so.sku_id;
