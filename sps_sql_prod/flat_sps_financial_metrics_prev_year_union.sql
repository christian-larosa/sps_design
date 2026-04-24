-- This table extracts and maintains the financial metrics mapping required for generating Supplier Scorecards.
-- SPS Execution: Position No. 5.2 (UNION VERSION - alternative to GROUPING SETS)
-- DML SCRIPT: SPS Refact Full Refresh with UNION optimization

-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'FP_HK|FP_PH|FP_SG|GV_ES|GV_IT|GV_UA|HF_EG|HS_SA|IN_AE|IN_EG|NP_HU|PY_AR|PY_CL|PY_PE|TB_AE|TB_BH|TB_JO|TB_KW|TB_OM|TB_QA|YS_TR';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-10-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_prev_year`
CLUSTER BY
   global_entity_id,
   join_time_period
AS

WITH base_data AS (
  SELECT
    global_entity_id,
    month,
    quarter_year,
    principal_supplier_id,
    supplier_id,
    brand_owner_name,
    brand_name,
    l1_master_category,
    l2_master_category,
    l3_master_category,
    total_price_paid_net_eur,
    total_price_paid_net_lc,
    COGS_eur,
    COGS_lc
  FROM `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_month`
  WHERE DATE(month) >= DATE_SUB(CAST(param_date_start AS DATE), INTERVAL 1 YEAR)
    AND DATE(month) < DATE_SUB(CAST(param_date_end AS DATE), INTERVAL 1 YEAR)
    AND REGEXP_CONTAINS(global_entity_id, param_global_entity_id)
),

-- ========== MONTHLY AGGREGATIONS ==========

-- MONTHLY: By principal_supplier_id
agg_m_principal_supplier AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    principal_supplier_id AS brand_sup,
    principal_supplier_id AS entity_key,
    'principal' AS division_type,
    'supplier' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC) AS Net_Sales_eur_LY,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC) AS Net_Sales_lc_LY,
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC) AS COGS_eur_LY,
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC) AS COGS_lc_LY,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC) AS front_margin_amt_eur_LY,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC) AS front_margin_amt_lc_LY
  FROM base_data
  GROUP BY global_entity_id, month, principal_supplier_id
),

-- MONTHLY: By supplier_id
agg_m_supplier AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    supplier_id AS brand_sup,
    supplier_id AS entity_key,
    'division' AS division_type,
    'supplier' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, supplier_id
),

-- MONTHLY: By brand_owner_name
agg_m_brand_owner AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    brand_owner_name AS brand_sup,
    brand_owner_name AS entity_key,
    'brand_owner' AS division_type,
    'supplier' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, brand_owner_name
),

-- MONTHLY: By principal_supplier_id + brand_name
agg_m_principal_brand AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    principal_supplier_id AS brand_sup,
    brand_name AS entity_key,
    'principal' AS division_type,
    'brand_name' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, principal_supplier_id, brand_name
),

-- MONTHLY: By supplier_id + brand_name
agg_m_supplier_brand AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    supplier_id AS brand_sup,
    brand_name AS entity_key,
    'division' AS division_type,
    'brand_name' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, supplier_id, brand_name
),

-- MONTHLY: By brand_owner_name + brand_name
agg_m_brand_owner_brand AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    brand_owner_name AS brand_sup,
    brand_name AS entity_key,
    'brand_owner' AS division_type,
    'brand_name' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, brand_owner_name, brand_name
),

-- MONTHLY: By brand_name alone
agg_m_brand_alone AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    brand_name AS brand_sup,
    brand_name AS entity_key,
    'brand_name' AS division_type,
    'brand_name' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, brand_name
),

-- MONTHLY: By principal_supplier_id + l1_master_category
agg_m_principal_l1 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    principal_supplier_id AS brand_sup,
    l1_master_category AS entity_key,
    'principal' AS division_type,
    'level_one' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, principal_supplier_id, l1_master_category
),

-- MONTHLY: By principal_supplier_id + l2_master_category
agg_m_principal_l2 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    principal_supplier_id AS brand_sup,
    l2_master_category AS entity_key,
    'principal' AS division_type,
    'level_two' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, principal_supplier_id, l2_master_category
),

-- MONTHLY: By principal_supplier_id + l3_master_category
agg_m_principal_l3 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    principal_supplier_id AS brand_sup,
    l3_master_category AS entity_key,
    'principal' AS division_type,
    'level_three' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, principal_supplier_id, l3_master_category
),

-- MONTHLY: By supplier_id + l1_master_category
agg_m_supplier_l1 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    supplier_id AS brand_sup,
    l1_master_category AS entity_key,
    'division' AS division_type,
    'level_one' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, supplier_id, l1_master_category
),

-- MONTHLY: By supplier_id + l2_master_category
agg_m_supplier_l2 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    supplier_id AS brand_sup,
    l2_master_category AS entity_key,
    'division' AS division_type,
    'level_two' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, supplier_id, l2_master_category
),

-- MONTHLY: By supplier_id + l3_master_category
agg_m_supplier_l3 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    supplier_id AS brand_sup,
    l3_master_category AS entity_key,
    'division' AS division_type,
    'level_three' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, supplier_id, l3_master_category
),

-- MONTHLY: By brand_owner_name + l1_master_category
agg_m_brand_owner_l1 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    brand_owner_name AS brand_sup,
    l1_master_category AS entity_key,
    'brand_owner' AS division_type,
    'level_one' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, brand_owner_name, l1_master_category
),

-- MONTHLY: By brand_owner_name + l2_master_category
agg_m_brand_owner_l2 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    brand_owner_name AS brand_sup,
    l2_master_category AS entity_key,
    'brand_owner' AS division_type,
    'level_two' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, brand_owner_name, l2_master_category
),

-- MONTHLY: By brand_owner_name + l3_master_category
agg_m_brand_owner_l3 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    brand_owner_name AS brand_sup,
    l3_master_category AS entity_key,
    'brand_owner' AS division_type,
    'level_three' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, brand_owner_name, l3_master_category
),

-- MONTHLY: By brand_name + l1_master_category
agg_m_brand_l1 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    brand_name AS brand_sup,
    l1_master_category AS entity_key,
    'brand_name' AS division_type,
    'level_one' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, brand_name, l1_master_category
),

-- MONTHLY: By brand_name + l2_master_category
agg_m_brand_l2 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    brand_name AS brand_sup,
    l2_master_category AS entity_key,
    'brand_name' AS division_type,
    'level_two' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, brand_name, l2_master_category
),

-- MONTHLY: By brand_name + l3_master_category
agg_m_brand_l3 AS (
  SELECT
    global_entity_id,
    CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,
    brand_name AS brand_sup,
    l3_master_category AS entity_key,
    'brand_name' AS division_type,
    'level_three' AS supplier_level,
    'Monthly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, month, brand_name, l3_master_category
),

-- ========== QUARTERLY AGGREGATIONS ==========

-- QUARTERLY: By principal_supplier_id
agg_q_principal_supplier AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    principal_supplier_id AS brand_sup,
    principal_supplier_id AS entity_key,
    'principal' AS division_type,
    'supplier' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, principal_supplier_id
),

-- QUARTERLY: By supplier_id
agg_q_supplier AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    supplier_id AS brand_sup,
    supplier_id AS entity_key,
    'division' AS division_type,
    'supplier' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, supplier_id
),

-- QUARTERLY: By brand_owner_name
agg_q_brand_owner AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    brand_owner_name AS brand_sup,
    brand_owner_name AS entity_key,
    'brand_owner' AS division_type,
    'supplier' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, brand_owner_name
),

-- QUARTERLY: By principal_supplier_id + brand_name
agg_q_principal_brand AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    principal_supplier_id AS brand_sup,
    brand_name AS entity_key,
    'principal' AS division_type,
    'brand_name' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, principal_supplier_id, brand_name
),

-- QUARTERLY: By supplier_id + brand_name
agg_q_supplier_brand AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    supplier_id AS brand_sup,
    brand_name AS entity_key,
    'division' AS division_type,
    'brand_name' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, supplier_id, brand_name
),

-- QUARTERLY: By brand_owner_name + brand_name
agg_q_brand_owner_brand AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    brand_owner_name AS brand_sup,
    brand_name AS entity_key,
    'brand_owner' AS division_type,
    'brand_name' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, brand_owner_name, brand_name
),

-- QUARTERLY: By brand_name alone
agg_q_brand_alone AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    brand_name AS brand_sup,
    brand_name AS entity_key,
    'brand_name' AS division_type,
    'brand_name' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, brand_name
),

-- QUARTERLY: By principal_supplier_id + l1_master_category
agg_q_principal_l1 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    principal_supplier_id AS brand_sup,
    l1_master_category AS entity_key,
    'principal' AS division_type,
    'level_one' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, principal_supplier_id, l1_master_category
),

-- QUARTERLY: By principal_supplier_id + l2_master_category
agg_q_principal_l2 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    principal_supplier_id AS brand_sup,
    l2_master_category AS entity_key,
    'principal' AS division_type,
    'level_two' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, principal_supplier_id, l2_master_category
),

-- QUARTERLY: By principal_supplier_id + l3_master_category
agg_q_principal_l3 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    principal_supplier_id AS brand_sup,
    l3_master_category AS entity_key,
    'principal' AS division_type,
    'level_three' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, principal_supplier_id, l3_master_category
),

-- QUARTERLY: By supplier_id + l1_master_category
agg_q_supplier_l1 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    supplier_id AS brand_sup,
    l1_master_category AS entity_key,
    'division' AS division_type,
    'level_one' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, supplier_id, l1_master_category
),

-- QUARTERLY: By supplier_id + l2_master_category
agg_q_supplier_l2 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    supplier_id AS brand_sup,
    l2_master_category AS entity_key,
    'division' AS division_type,
    'level_two' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, supplier_id, l2_master_category
),

-- QUARTERLY: By supplier_id + l3_master_category
agg_q_supplier_l3 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    supplier_id AS brand_sup,
    l3_master_category AS entity_key,
    'division' AS division_type,
    'level_three' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, supplier_id, l3_master_category
),

-- QUARTERLY: By brand_owner_name + l1_master_category
agg_q_brand_owner_l1 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    brand_owner_name AS brand_sup,
    l1_master_category AS entity_key,
    'brand_owner' AS division_type,
    'level_one' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, brand_owner_name, l1_master_category
),

-- QUARTERLY: By brand_owner_name + l2_master_category
agg_q_brand_owner_l2 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    brand_owner_name AS brand_sup,
    l2_master_category AS entity_key,
    'brand_owner' AS division_type,
    'level_two' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, brand_owner_name, l2_master_category
),

-- QUARTERLY: By brand_owner_name + l3_master_category
agg_q_brand_owner_l3 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    brand_owner_name AS brand_sup,
    l3_master_category AS entity_key,
    'brand_owner' AS division_type,
    'level_three' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, brand_owner_name, l3_master_category
),

-- QUARTERLY: By brand_name + l1_master_category
agg_q_brand_l1 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    brand_name AS brand_sup,
    l1_master_category AS entity_key,
    'brand_name' AS division_type,
    'level_one' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, brand_name, l1_master_category
),

-- QUARTERLY: By brand_name + l2_master_category
agg_q_brand_l2 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    brand_name AS brand_sup,
    l2_master_category AS entity_key,
    'brand_name' AS division_type,
    'level_two' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, brand_name, l2_master_category
),

-- QUARTERLY: By brand_name + l3_master_category
agg_q_brand_l3 AS (
  SELECT
    global_entity_id,
    CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING)) AS join_time_period,
    brand_name AS brand_sup,
    l3_master_category AS entity_key,
    'brand_name' AS division_type,
    'level_three' AS supplier_level,
    'Quarterly' AS time_granularity,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_eur),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur) - SUM(COGS_eur),0), 2) AS NUMERIC),
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc) - SUM(COGS_lc),0), 2) AS NUMERIC)
  FROM base_data
  GROUP BY global_entity_id, quarter_year, brand_name, l3_master_category
)

-- ========== FINAL UNION ALL ==========
SELECT * FROM agg_m_principal_supplier
UNION ALL
SELECT * FROM agg_m_supplier
UNION ALL
SELECT * FROM agg_m_brand_owner
UNION ALL
SELECT * FROM agg_m_principal_brand
UNION ALL
SELECT * FROM agg_m_supplier_brand
UNION ALL
SELECT * FROM agg_m_brand_owner_brand
UNION ALL
SELECT * FROM agg_m_brand_alone
UNION ALL
SELECT * FROM agg_m_principal_l1
UNION ALL
SELECT * FROM agg_m_principal_l2
UNION ALL
SELECT * FROM agg_m_principal_l3
UNION ALL
SELECT * FROM agg_m_supplier_l1
UNION ALL
SELECT * FROM agg_m_supplier_l2
UNION ALL
SELECT * FROM agg_m_supplier_l3
UNION ALL
SELECT * FROM agg_m_brand_owner_l1
UNION ALL
SELECT * FROM agg_m_brand_owner_l2
UNION ALL
SELECT * FROM agg_m_brand_owner_l3
UNION ALL
SELECT * FROM agg_m_brand_l1
UNION ALL
SELECT * FROM agg_m_brand_l2
UNION ALL
SELECT * FROM agg_m_brand_l3
UNION ALL
SELECT * FROM agg_q_principal_supplier
UNION ALL
SELECT * FROM agg_q_supplier
UNION ALL
SELECT * FROM agg_q_brand_owner
UNION ALL
SELECT * FROM agg_q_principal_brand
UNION ALL
SELECT * FROM agg_q_supplier_brand
UNION ALL
SELECT * FROM agg_q_brand_owner_brand
UNION ALL
SELECT * FROM agg_q_brand_alone
UNION ALL
SELECT * FROM agg_q_principal_l1
UNION ALL
SELECT * FROM agg_q_principal_l2
UNION ALL
SELECT * FROM agg_q_principal_l3
UNION ALL
SELECT * FROM agg_q_supplier_l1
UNION ALL
SELECT * FROM agg_q_supplier_l2
UNION ALL
SELECT * FROM agg_q_supplier_l3
UNION ALL
SELECT * FROM agg_q_brand_owner_l1
UNION ALL
SELECT * FROM agg_q_brand_owner_l2
UNION ALL
SELECT * FROM agg_q_brand_owner_l3
UNION ALL
SELECT * FROM agg_q_brand_l1
UNION ALL
SELECT * FROM agg_q_brand_l2
UNION ALL
SELECT * FROM agg_q_brand_l3
