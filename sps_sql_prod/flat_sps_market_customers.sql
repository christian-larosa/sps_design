-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'FP_HK|FP_PH|FP_SG|GV_ES|GV_IT|GV_UA|HF_EG|HS_SA|IN_AE|IN_EG|NP_HU|PY_AR|PY_CL|PY_PE|TB_AE|TB_BH|TB_JO|TB_KW|TB_OM|TB_QA|YS_TR';
DECLARE param_country_code STRING DEFAULT r'hk|ph|sg|es|it|ua|eg|sa|ae|hu|ar|cl|pe|bh|jo|kw|om|qa|tr';
DECLARE param_date_start   DATE   DEFAULT DATE('2025-10-01');
DECLARE param_date_end     DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

-- This table extracts market customers metrics required for generating Supplier Scorecards.
-- SPS Execution: Position No. 13
-- Propósito: denominador para customer_penetration en segmentación
-- Grain: global_entity_id × time_period × time_granularity
-- Monthly:  clientes únicos en el mes (sin filtro de supplier)
-- Quarterly: clientes únicos en el trimestre (sin filtro de supplier)
-- Nota: quarterly NO es suma de meses — es COUNT DISTINCT sobre el periodo completo
-- Un cliente que compró en oct+nov+dic = 1 cliente en Q4, no 3
-- ============================================================

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_market_customers`
AS

WITH

-- Base: todas las órdenes del periodo, sin filtro de supplier
-- Grain: order_id × analytical_customer_id × fecha
base AS (
  SELECT
    global_entity_id,
    analytical_customer_id,
    order_id,
    CAST(DATE_TRUNC(order_date, MONTH) AS STRING) AS month,
    CAST(CONCAT(
      'Q', EXTRACT(QUARTER FROM order_date),
      '-', EXTRACT(YEAR FROM order_date)
    ) AS STRING) AS quarter_year
  FROM `dh-darkstores-live.csm_automated_tables.sps_customer_order`
  WHERE REGEXP_CONTAINS(global_entity_id, param_global_entity_id)
    AND REGEXP_CONTAINS(country_code, param_country_code)
    AND order_date BETWEEN param_date_start AND param_date_end
  -- Sin GROUP BY de supplier — queremos todos los clientes de la plataforma
  -- independientemente de qué supplier compraron
),

-- Monthly: una fila por entidad × mes
monthly AS (
  SELECT
    global_entity_id,
    month                                         AS time_period,
    'Monthly'                                     AS time_granularity,
    COUNT(DISTINCT analytical_customer_id)        AS total_market_customers,
    COUNT(DISTINCT order_id)                      AS total_market_orders
  FROM base
  GROUP BY global_entity_id, month
),

-- Quarterly: una fila por entidad × trimestre
-- COUNT DISTINCT sobre todo el quarter — no suma de meses
-- Semánticamente correcto: un cliente que compró en 3 meses del quarter = 1 cliente
quarterly AS (
  SELECT
    global_entity_id,
    quarter_year                                  AS time_period,
    'Quarterly'                                   AS time_granularity,
    COUNT(DISTINCT analytical_customer_id)        AS total_market_customers,
    COUNT(DISTINCT order_id)                      AS total_market_orders
  FROM base
  GROUP BY global_entity_id, quarter_year
)

SELECT * FROM monthly
UNION ALL
SELECT * FROM quarterly
