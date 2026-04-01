-- This table extracts and maintains the efficiency metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Final Position 
-- DML SCRIPT: SPS Refact Incremental Refresh for dh-darkstores-live.csm_automated_tables.sps_score_tableau

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_score_tableau`
CLUSTER BY
   global_entity_id
AS 
WITH
  -- ============================================================
  -- all_keys: UNION ALL de las 8 tablas → DISTINCT de las 7 llaves
  -- Genera el universo completo de combinaciones únicas.
  -- Cada tabla aporta sus combinaciones de tiempo × jerarquía.
  -- El DISTINCT final elimina duplicados entre tablas.
  -- ============================================================
  all_keys AS (
SELECT DISTINCT * FROM (
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `dh-darkstores-live.csm_automated_tables.sps_price_index`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `dh-darkstores-live.csm_automated_tables.sps_days_payable`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `dh-darkstores-live.csm_automated_tables.sps_financial_metrics`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `dh-darkstores-live.csm_automated_tables.sps_line_rebate_metrics`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `dh-darkstores-live.csm_automated_tables.sps_efficiency`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `dh-darkstores-live.csm_automated_tables.sps_listed_sku`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `dh-darkstores-live.csm_automated_tables.sps_shrinkage`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `dh-darkstores-live.csm_automated_tables.sps_delivery_costs`
))
-- ============================================================
-- SELECT final: all_keys como spine + 8 LEFT JOINs
-- Llave de join en todos: (global_entity_id, time_period,
--   time_granularity, division_type, supplier_level,
--   entity_key, brand_sup)
-- ============================================================
SELECT o.*,
 -- ── price_index ─────────────────────────────────────────────
 p.median_price_index,
 p.price_index_numerator,   -- NUEVO: numerador para recalcular median_price_index en Tableau
 p.price_index_weight,      -- NUEVO: denominador para recalcular median_price_index en Tableau
 -- ── days_payable ────────────────────────────────────────────
 dpo.payment_days,
 dpo.doh,
 dpo.dpo,
 dpo.stock_value_eur,       -- NUEVO: ingrediente para recalcular doh en Tableau
 dpo.cogs_monthly_eur,      -- NUEVO: ingrediente para recalcular doh en Tableau
 dpo.days_in_month,         -- NUEVO: ingrediente para recalcular doh mensual en Tableau
 dpo.days_in_quarter,       -- NUEVO: ingrediente para recalcular doh trimestral en Tableau
 -- ── financial_metrics (via .*) ───────────────────────────────
 -- Incluye: Net_Sales_eur/lc, COGS_eur/lc, front_margin_amt_eur/lc,
 -- total_supplier_funding_eur/lc, Net_Sales_from_promo_eur/lc,
 -- Net_Sales_eur/lc_Last_Year, YoY_GPV_Growth, Front_Margin_eur/lc,
 -- Total_Margin_LC, back_margin_amt_lc, Promo_GPV_contribution_eur/lc
 sfm.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup),
 -- ── line_rebate_metrics (via .*) ─────────────────────────────
 -- Incluye: total_rebate, total_rebate_wo_dist_allowance_lc,
 -- calc_gross_delivered, calc_gross_return,   ← NUEVOS
 -- calc_net_delivered, calc_net_return         ← NUEVOS
 slrm.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup, net_purchase),
 -- ── efficiency (via .*) ──────────────────────────────────────
 -- Incluye: sku_listed, zero_movers, slow_movers, efficient_movers,
 -- la_zero_movers, la_slow_movers, new_*, sold_items, gpv_eur
 se.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup),
 -- ── listed_sku ───────────────────────────────────────────────
 listed.listed_skus,
 -- ── shrinkage ────────────────────────────────────────────────
 -- spoilage_value y retail_revenue ya viajan desde producción
 shrink.spoilage_value,
 shrink.retail_revenue,
 shrink.spoilage_rate,
 -- ── delivery_costs ───────────────────────────────────────────
 deliv.delivery_cost_eur,
 deliv.delivery_cost_local
FROM all_keys AS o
-- ── JOIN 1: price_index ──────────────────────────────────────
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_price_index` AS p
  ON o.global_entity_id = p.global_entity_id AND o.time_period = p.time_period AND o.time_granularity = p.time_granularity AND o.division_type = p.division_type AND o.supplier_level = p.supplier_level AND o.entity_key = p.entity_key AND o.brand_sup = p.brand_sup
-- ── JOIN 2: days_payable ─────────────────────────────────────
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_days_payable` AS dpo
  ON o.global_entity_id = dpo.global_entity_id AND o.time_period = dpo.time_period AND o.time_granularity = dpo.time_granularity AND o.division_type = dpo.division_type AND o.supplier_level = dpo.supplier_level AND o.entity_key = dpo.entity_key AND o.brand_sup = dpo.brand_sup
-- ── JOIN 3: financial_metrics ────────────────────────────────
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_financial_metrics` AS sfm
  ON o.global_entity_id = sfm.global_entity_id AND o.time_period = sfm.time_period AND o.time_granularity = sfm.time_granularity AND o.division_type = sfm.division_type AND o.supplier_level = sfm.supplier_level AND o.entity_key = sfm.entity_key AND o.brand_sup = sfm.brand_sup
-- ── JOIN 4: line_rebate_metrics ──────────────────────────────
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_line_rebate_metrics` AS slrm
  ON o.global_entity_id = slrm.global_entity_id AND o.time_period = slrm.time_period AND o.time_granularity = slrm.time_granularity AND o.division_type = slrm.division_type AND o.supplier_level = slrm.supplier_level AND o.entity_key = slrm.entity_key AND o.brand_sup = slrm.brand_sup
-- ── JOIN 5: efficiency ───────────────────────────────────────
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_efficiency` AS se
  ON o.global_entity_id = se.global_entity_id AND o.time_period = se.time_period AND o.time_granularity = se.time_granularity AND o.division_type = se.division_type AND o.supplier_level = se.supplier_level AND o.entity_key = se.entity_key AND o.brand_sup = se.brand_sup
-- ── JOIN 6: listed_sku ───────────────────────────────────────
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_listed_sku` AS listed
  ON o.global_entity_id = listed.global_entity_id AND o.time_period = listed.time_period AND o.time_granularity = listed.time_granularity AND o.division_type = listed.division_type AND o.supplier_level = listed.supplier_level AND o.entity_key = listed.entity_key AND o.brand_sup = listed.brand_sup
-- ── JOIN 7: shrinkage ────────────────────────────────────────
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_shrinkage` AS shrink
  ON o.global_entity_id = shrink.global_entity_id AND o.time_period = shrink.time_period AND o.time_granularity = shrink.time_granularity AND o.division_type = shrink.division_type AND o.supplier_level = shrink.supplier_level AND o.entity_key = shrink.entity_key AND o.brand_sup = shrink.brand_sup
-- ── JOIN 8: delivery_costs ───────────────────────────────────
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_delivery_costs` AS deliv
  ON o.global_entity_id = deliv.global_entity_id AND o.time_period = deliv.time_period AND o.time_granularity = deliv.time_granularity AND o.division_type = deliv.division_type AND o.supplier_level = deliv.supplier_level AND o.entity_key = deliv.entity_key AND o.brand_sup = deliv.brand_sup