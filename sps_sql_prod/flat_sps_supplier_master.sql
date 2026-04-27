CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_supplier_master`
CLUSTER BY global_entity_id, time_period
AS

WITH

-- ── Supplier names mapping: limpiar sps_product para traducir supplier_id → supplier_name ──
sps_product_clean AS (
  SELECT DISTINCT
    CAST(supplier_id AS STRING) AS supplier_id,
    supplier_name,
    global_entity_id
  FROM `dh-darkstores-live.csm_automated_tables.sps_product`
  WHERE supplier_id IS NOT NULL
),

base AS (
  SELECT
    -- ── Identidad ─────────────────────────────────────────────────────────
    st.global_entity_id,
    st.time_period,
    st.time_granularity,
    st.division_type,
    st.supplier_level,
    st.entity_key,
    st.brand_sup,

    -- ── Financieros base ──────────────────────────────────────────────────
    SUM(st.Net_Sales_lc)                    AS Net_Sales_lc,
    SUM(st.Net_Sales_eur)                   AS Net_Sales_eur,
    SUM(st.Net_Sales_lc_Last_Year)          AS Net_Sales_lc_Last_Year,
    SUM(st.Net_Sales_eur_Last_Year)         AS Net_Sales_eur_Last_Year,
    SUM(st.COGS_lc)                         AS COGS_lc,
    SUM(st.front_margin_amt_lc)             AS front_margin_amt_lc,
    SUM(st.back_margin_amt_lc)              AS back_margin_amt_lc,
    SUM(st.total_supplier_funding_lc)       AS total_supplier_funding_lc,
    SUM(st.total_discount_lc)               AS total_discount_lc,
    SUM(st.Net_Sales_from_promo_lc)         AS Net_Sales_from_promo_lc,
    SUM(st.total_orders)                    AS total_orders,
    SUM(st.total_customers)                 AS total_customers,
    MAX(st.total_market_customers)          AS total_market_customers,
    SUM(st.Total_Net_Sales_lc_order)        AS Total_Net_Sales_lc_order,

    -- ── Efficiency ingredients ────────────────────────────────────────────
    SUM(st.weight_efficiency)               AS weight_efficiency,
    SUM(st.gpv_eur)                         AS gpv_eur,

    -- ── Price index ingredients ───────────────────────────────────────────
    SUM(st.price_index_numerator)           AS price_index_numerator,
    SUM(st.price_index_weight)              AS price_index_weight,

    -- ── Purchase order ingredients ────────────────────────────────────────
    SUM(st.on_time_orders)                  AS on_time_orders,
    SUM(st.total_non_cancelled__po_orders)  AS total_non_cancelled_po_orders,
    SUM(st.total_received_qty_per_po_order) AS total_received_qty,
    SUM(st.total_demanded_qty_per_po_order) AS total_demanded_qty,
    SUM(st.total_cancelled_po_orders)       AS total_cancelled_po_orders,

    -- ── Listed SKUs ───────────────────────────────────────────────────────
    MAX(st.listed_skus)                     AS listed_skus,

    -- ── Shrinkage ─────────────────────────────────────────────────────────
    SUM(st.spoilage_value_eur)              AS spoilage_value_eur,
    SUM(st.retail_revenue_eur)              AS retail_revenue_eur,

    -- ── Days payable ingredients ──────────────────────────────────────────
    SUM(st.stock_value_eur)                 AS stock_value_eur,
    SUM(st.cogs_monthly_eur)                AS cogs_monthly_eur,
    MAX(st.days_in_month)                   AS days_in_month,

    -- ── Delivery costs ────────────────────────────────────────────────────
    SUM(st.delivery_cost_eur)               AS delivery_cost_eur,
    SUM(st.delivery_cost_local)             AS delivery_cost_local

  FROM `dh-darkstores-live.csm_automated_tables.sps_score_tableau` st
  WHERE st.supplier_level   = 'supplier'
    AND st.time_granularity = 'Monthly'
    AND st.division_type    IN ('division', 'principal')
  GROUP BY
    st.global_entity_id, st.time_period, st.time_granularity,
    st.division_type, st.supplier_level, st.entity_key, st.brand_sup
)

SELECT
  b.*,

  -- ── Supplier name (from sps_product) ───────────────────────────────────
  CASE
    WHEN b.division_type IN ('division', 'principal') THEN COALESCE(p.supplier_name, b.entity_key, 'Unknown')
    ELSE COALESCE(b.entity_key, 'Unknown')
  END AS supplier_name,

  -- ── Entity brand name (mapped from global_entity_id prefix) ────────────
  CASE
    WHEN SUBSTR(b.global_entity_id, 1, 2) = 'PY' THEN 'PedidosYa'
    WHEN SUBSTR(b.global_entity_id, 1, 2) = 'HS' THEN 'HungerStation'
    WHEN SUBSTR(b.global_entity_id, 1, 2) = 'TB' THEN 'Talabat'
    WHEN SUBSTR(b.global_entity_id, 1, 2) IN ('FP', 'FD', 'YS') THEN 'Pandora'
    WHEN SUBSTR(b.global_entity_id, 1, 2) = 'GV' THEN 'Glovo'
    WHEN SUBSTR(b.global_entity_id, 1, 2) IN ('IN', 'NP', 'HF') THEN 'Other'
    ELSE 'Unknown'
  END AS global_entity_name,

  -- ── Ratios calculados ─────────────────────────────────────────────────
  ROUND(SAFE_DIVIDE(b.on_time_orders,
    NULLIF(b.total_non_cancelled_po_orders, 0)), 4)               AS ratio_otd,
  ROUND(SAFE_DIVIDE(b.total_received_qty,
    NULLIF(b.total_demanded_qty, 0)), 4)                           AS ratio_fill_rate,
  ROUND(SAFE_DIVIDE(b.weight_efficiency,
    NULLIF(b.gpv_eur, 0)), 4)                                      AS ratio_efficiency,
  ROUND(SAFE_DIVIDE(b.price_index_numerator,
    NULLIF(b.price_index_weight, 0)), 4)                           AS ratio_price_index,
  ROUND(SAFE_DIVIDE(
    b.Net_Sales_lc - b.Net_Sales_lc_Last_Year,
    NULLIF(b.Net_Sales_lc_Last_Year, 0)), 4)                       AS ratio_yoy,
  ROUND(SAFE_DIVIDE(b.back_margin_amt_lc,
    NULLIF(b.Net_Sales_lc, 0)), 4)                                 AS ratio_back_margin,
  ROUND(SAFE_DIVIDE(
    b.front_margin_amt_lc + b.total_supplier_funding_lc,
    NULLIF(b.Net_Sales_lc, 0)), 4)                                 AS ratio_front_margin,
  ROUND(SAFE_DIVIDE(b.total_discount_lc,
    NULLIF(b.Net_Sales_lc + b.total_discount_lc, 0)), 4)           AS ratio_gbd,
  ROUND(SAFE_DIVIDE(b.Net_Sales_from_promo_lc,
    NULLIF(b.Net_Sales_lc, 0)), 4)                                 AS ratio_promo_contribution,
  ROUND(SAFE_DIVIDE(b.Total_Net_Sales_lc_order,
    NULLIF(b.total_orders, 0)), 4)                                 AS ratio_abv,
  ROUND(SAFE_DIVIDE(b.total_orders,
    NULLIF(b.total_customers, 0)), 4)                              AS ratio_frequency,
  ROUND(SAFE_DIVIDE(b.total_customers,
    NULLIF(b.total_market_customers, 0)) * 100, 4)                 AS ratio_customer_penetration,
  ROUND(SAFE_DIVIDE(b.spoilage_value_eur,
    NULLIF(b.retail_revenue_eur, 0)), 4)                           AS ratio_spoilage,
  ROUND(SAFE_DIVIDE(b.stock_value_eur,
    NULLIF(b.cogs_monthly_eur / NULLIF(b.days_in_month, 0), 0)), 1) AS ratio_doh,
  ROUND(SAFE_DIVIDE(b.delivery_cost_local,
    NULLIF(b.Net_Sales_lc, 0)), 4)                                 AS ratio_delivery_cost,
  ROUND(SAFE_DIVIDE(
    b.Net_Sales_lc + b.total_supplier_funding_lc
    - b.COGS_lc + b.back_margin_amt_lc,
    NULLIF(b.Net_Sales_lc, 0)), 4)                                 AS ratio_net_profit_margin,

  -- ── Scores individuales (de sps_supplier_scoring) ─────────────────────
  sc.score_fill_rate,
  sc.score_otd,
  sc.score_yoy,
  sc.score_efficiency,
  sc.score_gbd,
  sc.score_back_margin,
  sc.score_front_margin,
  sc.operations_score,
  sc.commercial_score,
  sc.total_score,

  -- ── Thresholds (explainability completa) ─────────────────────────────
  sc.threshold_yoy_max,
  sc.threshold_bm_start,
  sc.threshold_bm_end,
  sc.threshold_fm_start,
  sc.threshold_fm_end,
  sc.threshold_gbd_target,
  sc.threshold_gbd_lower,
  sc.threshold_gbd_upper,

  -- ── Segmentación ──────────────────────────────────────────────────────
  seg.segment_lc,
  seg.importance_score_lc,
  seg.productivity_score_lc,
  seg.abv_score_lc,
  seg.frequency_score,
  seg.customer_penetration_score,

  -- ── Weighted scores (ponderados por Net_Sales_lc) ─────────────────────
  -- Numeradores — para poder agregar correctamente en Tableau
  -- weighted_X = score_X * Net_Sales_lc → SUM(num)/SUM(Net_Sales_lc) da el weighted avg
  ROUND(sc.score_fill_rate    * b.Net_Sales_lc, 4) AS wscore_num_fill_rate,
  ROUND(sc.score_otd          * b.Net_Sales_lc, 4) AS wscore_num_otd,
  ROUND(sc.score_yoy          * b.Net_Sales_lc, 4) AS wscore_num_yoy,
  ROUND(sc.score_efficiency   * b.Net_Sales_lc, 4) AS wscore_num_efficiency,
  ROUND(sc.score_gbd          * b.Net_Sales_lc, 4) AS wscore_num_gbd,
  ROUND(sc.score_back_margin  * b.Net_Sales_lc, 4) AS wscore_num_back_margin,
  ROUND(sc.score_front_margin * b.Net_Sales_lc, 4) AS wscore_num_front_margin,
  ROUND(sc.operations_score   * b.Net_Sales_lc, 4) AS wscore_num_operations,
  ROUND(sc.commercial_score   * b.Net_Sales_lc, 4) AS wscore_num_commercial,
  ROUND(sc.total_score        * b.Net_Sales_lc, 4) AS wscore_num_total,
  -- Denominador compartido para todos los weighted scores
  b.Net_Sales_lc                                   AS wscore_denom

FROM base b
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_supplier_scoring` sc
  ON  b.global_entity_id = sc.global_entity_id
  AND b.time_period      = sc.time_period
  AND b.time_granularity = sc.time_granularity
  AND b.division_type    = sc.division_type
  AND b.supplier_level   = sc.supplier_level
  AND b.entity_key       = sc.entity_key
  AND b.brand_sup        = sc.brand_sup
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_supplier_segmentation` seg
  ON  b.global_entity_id = seg.global_entity_id
  AND b.time_period      = seg.time_period
  AND b.time_granularity = seg.time_granularity
  AND b.division_type    = seg.division_type
  AND b.supplier_level   = seg.supplier_level
  AND b.entity_key       = seg.entity_key
  AND b.brand_sup        = seg.brand_sup
LEFT JOIN sps_product_clean p
  ON  b.entity_key       = p.supplier_id
  AND b.global_entity_id = p.global_entity_id
  AND b.division_type    IN ('division', 'principal')