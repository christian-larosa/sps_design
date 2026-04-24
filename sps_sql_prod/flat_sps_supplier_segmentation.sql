-- This table classifies suppliers based on sps_score_tableau metrics.
-- SPS Execution: Position No. 14b
-- Propósito: clasificar suppliers en Key Accounts / Standard /
--            Niche / Long Tail usando dos ejes:
--   Importancia  = Net Profit LC (percentil vs peers del mercado)
--   Productividad = ABV + Frequency + Customer Penetration (percentil)
-- Scope: supplier_level='supplier', Monthly, division+principal+brand_owner
-- Fuente: sps_score_tableau + sps_market_customers (ya joineado)
-- Replica: old SPS _srm_supplier_scorecard_supplier_segmentation_rough.sql
-- Pesos de Productividad (rebalanceados):
--   ABV (cesta) = 30 puntos (era 50)
--   Frequency (lealtad) = 30 puntos (era 30)
--   Customer Penetration (alcance) = 40 puntos (era 20, ahora determinante)
-- Diferencias vs old SPS:
--   (1) Fuente: sps_score_tableau en lugar de _srm_supplier_scorecard_supplier_rough
--   (2) Efficiency: AQS v7 (weight_efficiency/gpv_eur) en lugar de v5
--   (3) total_market_customers viene de sps_market_customers via sps_score_tableau
--   (4) Pesos de productividad rebalanceados para priorizar penetration real en mercado
-- ============================================================

-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'FP_HK|FP_PH|FP_SG|GV_ES|GV_IT|GV_UA|HF_EG|HS_SA|IN_AE|IN_EG|NP_HU|PY_AR|PY_CL|PY_PE|TB_AE|TB_BH|TB_JO|TB_KW|TB_OM|TB_QA|YS_TR';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-10-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_supplier_segmentation`
AS

WITH

-- ── Base: leer de sps_score_tableau, filtrar scope ───────────────────────────
base AS (
  SELECT
    global_entity_id,
    time_period,
    time_granularity,
    division_type,
    supplier_level,
    entity_key,
    brand_sup,

    -- Métricas para Importancia (eje 1)
    -- Net_profit_LC = (Net_Sales_lc + funding_lc - COGS_lc) + back_margin_amt_lc
    -- Replica exacta de la fórmula del old SPS
    COALESCE(Net_Sales_lc, 0)
      + COALESCE(total_supplier_funding_lc, 0)
      - COALESCE(COGS_lc, 0)
      + COALESCE(back_margin_amt_lc, 0)                     AS net_profit_lc,

    -- Métricas para Productividad (eje 2)
    -- ABV: valor de canasta cuando el cliente compra este supplier
    -- FIX aplicado: Total_Net_Sales_lc_order ya deduplicado (ROW_NUMBER fix)
    SAFE_DIVIDE(
      Total_Net_Sales_lc_order,
      NULLIF(total_orders, 0)
    )                                                        AS abv_lc_order,

    -- Frequency: órdenes por cliente por mes
    SAFE_DIVIDE(
      total_orders,
      NULLIF(total_customers, 0)
    )                                                        AS frequency,

    -- Customer penetration: % de clientes de la plataforma que compraron este supplier
    SAFE_DIVIDE(
      total_customers,
      NULLIF(total_market_customers, 0)
    ) * 100                                                  AS customer_penetration,

    -- GPV filter flag: excluir de percentiles si < 1,000 EUR
    -- Replica exacta del old SPS IS_GPV_LESS_THAN_1K
    CASE
      WHEN COALESCE(Net_Sales_eur, 0) < 1000 THEN 'Not Applicable'
      ELSE 'OK'
    END                                                      AS gpv_flag,

    -- Market context: denominadores para penetración
    -- Requeridos para análisis autocontenido sin JOIN externo
    total_customers,
    total_orders,
    total_market_customers,
    Net_Sales_eur,
    Net_Sales_lc

  FROM `dh-darkstores-live.csm_automated_tables.sps_score_tableau`
  WHERE supplier_level   = 'supplier'
    AND time_granularity = 'Monthly'
    AND division_type    IN ('division', 'principal', 'brand_owner')
),

-- ── Percentiles: calcular p15 y p95 solo sobre suppliers con GPV >= 1,000 EUR ─
-- Replica exacta del CTE percentiles del old SPS
-- PARTITION BY: global_entity_id + time_period + time_granularity + division_type
-- Mismo scope que el old SPS — percentiles son locales al mercado+periodo+division
-- DISTINCT: Una sola fila por partición (los percentiles son propiedades del mercado, no del supplier)
percentiles AS (
  SELECT DISTINCT
    global_entity_id,
    time_period,
    time_granularity,
    division_type,

    -- Eje 1: Importancia
    ROUND(PERCENTILE_CONT(net_profit_lc, 0.95)
      OVER (PARTITION BY global_entity_id, time_period, time_granularity, division_type), 2)
      AS p95_net_profit_lc,
    ROUND(PERCENTILE_CONT(net_profit_lc, 0.15)
      OVER (PARTITION BY global_entity_id, time_period, time_granularity, division_type), 2)
      AS p15_net_profit_lc,

    -- Eje 2: Productividad — ABV
    ROUND(PERCENTILE_CONT(abv_lc_order, 0.95)
      OVER (PARTITION BY global_entity_id, time_period, time_granularity, division_type), 2)
      AS p95_abv_lc,
    ROUND(PERCENTILE_CONT(abv_lc_order, 0.15)
      OVER (PARTITION BY global_entity_id, time_period, time_granularity, division_type), 2)
      AS p15_abv_lc,

    -- Eje 2: Productividad — Frequency
    ROUND(PERCENTILE_CONT(frequency, 0.95)
      OVER (PARTITION BY global_entity_id, time_period, time_granularity, division_type), 2)
      AS p95_frequency,
    ROUND(PERCENTILE_CONT(frequency, 0.15)
      OVER (PARTITION BY global_entity_id, time_period, time_granularity, division_type), 2)
      AS p15_frequency,

    -- Eje 2: Productividad — Customer Penetration
    ROUND(PERCENTILE_CONT(customer_penetration, 0.95)
      OVER (PARTITION BY global_entity_id, time_period, time_granularity, division_type), 2)
      AS p95_customer_penetration,
    ROUND(PERCENTILE_CONT(customer_penetration, 0.15)
      OVER (PARTITION BY global_entity_id, time_period, time_granularity, division_type), 2)
      AS p15_customer_penetration

  FROM base
  WHERE gpv_flag = 'OK'  -- solo suppliers con GPV >= 1,000 EUR entran al cálculo
),

-- ── Scoring: asignar puntos a cada supplier ───────────────────────────────────
scoring AS (
  SELECT
    b.*,
    p.p95_net_profit_lc,
    p.p15_net_profit_lc,

    -- ── EJE 1: IMPORTANCIA (0-100 puntos) ──────────────────────────────────
    -- Replica exacta del old SPS retail_profit_score_lc
    -- Suppliers con gpv_flag = 'Not Applicable' reciben score 0 (modelo excluye pequeños)
    CASE
      WHEN b.gpv_flag = 'Not Applicable' THEN 0.0
      ELSE ROUND(COALESCE(CASE
        WHEN b.net_profit_lc >= p.p95_net_profit_lc THEN 100.0
        WHEN b.net_profit_lc <= p.p15_net_profit_lc THEN 0.0
        ELSE SAFE_DIVIDE(
          b.net_profit_lc - p.p15_net_profit_lc,
          p.p95_net_profit_lc - p.p15_net_profit_lc
        ) * 100
      END, 0), 3)
    END                                                      AS importance_score_lc,

    -- ── EJE 2: PRODUCTIVIDAD — componentes ─────────────────────────────────

    -- ABV score (peso 30 puntos) — balanceado con penetration
    -- Suppliers con gpv_flag = 'Not Applicable' reciben score 0
    CASE
      WHEN b.gpv_flag = 'Not Applicable' THEN 0.0
      ELSE ROUND(COALESCE(CASE
        WHEN b.abv_lc_order >= p.p95_abv_lc THEN 30.0
        WHEN b.abv_lc_order <= p.p15_abv_lc THEN 0.0
        ELSE SAFE_DIVIDE(
          b.abv_lc_order - p.p15_abv_lc,
          p.p95_abv_lc - p.p15_abv_lc
        ) * 30
      END, 0), 3)
    END                                                      AS abv_score_lc,

    -- Frequency score (peso 30 puntos) — igual a ABV, complementario
    -- Suppliers con gpv_flag = 'Not Applicable' reciben score 0
    CASE
      WHEN b.gpv_flag = 'Not Applicable' THEN 0.0
      ELSE ROUND(COALESCE(CASE
        WHEN b.frequency >= p.p95_frequency THEN 30.0
        WHEN b.frequency <= p.p15_frequency THEN 0.0
        ELSE SAFE_DIVIDE(
          b.frequency - p.p15_frequency,
          p.p95_frequency - p.p15_frequency
        ) * 30
      END, 0), 3)
    END                                                      AS frequency_score,

    -- Customer penetration score (peso 40 puntos) — determinante
    -- Penetration es el mejor indicador de alcance real en el mercado
    -- Suppliers con gpv_flag = 'Not Applicable' reciben score 0
    CASE
      WHEN b.gpv_flag = 'Not Applicable' THEN 0.0
      ELSE ROUND(COALESCE(CASE
        WHEN b.customer_penetration >= p.p95_customer_penetration THEN 40.0
        WHEN b.customer_penetration <= p.p15_customer_penetration THEN 0.0
        ELSE SAFE_DIVIDE(
          b.customer_penetration - p.p15_customer_penetration,
          p.p95_customer_penetration - p.p15_customer_penetration
        ) * 40
      END, 0), 3)
    END                                                      AS customer_penetration_score

  FROM base b
  LEFT JOIN percentiles p
    ON  b.global_entity_id  = p.global_entity_id
    AND b.time_period       = p.time_period
    AND b.time_granularity  = p.time_granularity
    AND b.division_type     = p.division_type
    -- Percentiles are market-level metrics, not supplier-specific
    -- All suppliers in the partition get the same p15/p95 values
),

-- ── Aggregation: sumar componentes de productividad ──────────────────────────
final AS (
  SELECT
    global_entity_id,
    time_period,
    time_granularity,
    division_type,
    supplier_level,
    entity_key,
    brand_sup,
    net_profit_lc,
    abv_lc_order,
    frequency,
    customer_penetration,
    gpv_flag,
    p95_net_profit_lc,
    p15_net_profit_lc,
    importance_score_lc,
    abv_score_lc,
    frequency_score,
    customer_penetration_score,
    -- Productivity score total (max 100 puntos)
    ROUND(abv_score_lc + frequency_score + customer_penetration_score, 3)
      AS productivity_score_lc,

    -- Clasificación final — replica exacta del old SPS
    -- Importancia > 15 = importante para DH
    -- Productividad >= 40 = productivo para el cliente
    CASE
      WHEN importance_score_lc > 15 AND (abv_score_lc + frequency_score + customer_penetration_score) >= 40
        THEN 'Key Accounts'
      WHEN importance_score_lc > 15 AND (abv_score_lc + frequency_score + customer_penetration_score) < 40
        THEN 'Standard'
      WHEN importance_score_lc <= 15 AND (abv_score_lc + frequency_score + customer_penetration_score) >= 40
        THEN 'Niche'
      ELSE 'Long Tail'
    END                                                      AS segment_lc,

    -- Market context: required for self-contained analysis
    total_customers,
    total_orders,
    total_market_customers,
    Net_Sales_eur,
    Net_Sales_lc

  FROM scoring
)

SELECT * FROM final
