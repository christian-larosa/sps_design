-- This table aggregates all metrics for Supplier Scorecards and exports to Tableau.
-- SPS Execution: Position No. 14a
-- Ingredientes desde sps_price_index:
--   price_index_numerator: SUM(median_bp_index * sku_gpv_eur)
--   price_index_weight: SUM(sku_gpv_eur)
-- Ingredientes desde sps_days_payable:
--   stock_value_eur, cogs_monthly_eur, days_in_month, days_in_quarter
--   [doh_monthly = SUM(stock_value_eur) / (SUM(cogs_monthly_eur) / MAX(days_in_month))]
-- Ingredientes desde sps_purchase_order:
--   total_po_orders, total_compliant_po_orders
--   total_received_qty_ALL, total_demanded_qty_ALL
--   (+ on_time_orders, fill_rate, otd, supplier_non_fulfilled_order_qty)
-- Ingredientes desde sps_efficiency:
--   weight_efficiency: GPV-weighted efficiency numerator (AQS v7 methodology)
--   gpv_eur: GPV denominator for weighted efficiency
--   Tableau formula: SUM(weight_efficiency) / SUM(gpv_eur) = efficiency %
--   efficient_movers, sku_listed (como listed_skus_efficiency)

-- ── PARAMS ───────────────────────────────────────────────────
DECLARE param_global_entity_id STRING DEFAULT r'FP_HK|FP_PH|FP_SG|GV_ES|GV_IT|GV_UA|HF_EG|HS_SA|IN_AE|IN_EG|NP_HU|PY_AR|PY_CL|PY_PE|TB_AE|TB_BH|TB_JO|TB_KW|TB_OM|TB_QA|YS_TR';
DECLARE param_date_start       DATE   DEFAULT DATE('2025-10-01');
DECLARE param_date_end         DATE   DEFAULT CURRENT_DATE();
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_score_tableau`
CLUSTER BY
   global_entity_id
AS
WITH
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
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `dh-darkstores-live.csm_automated_tables.sps_purchase_order`
))
SELECT o.*,
 p.median_price_index,
 p.price_index_numerator,
 p.price_index_weight,
 dpo.payment_days,
 dpo.doh,
 dpo.dpo,
 dpo.stock_value_eur,
 dpo.cogs_monthly_eur,
 dpo.days_in_month,
 dpo.days_in_quarter,
 sfm.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup),
 slrm.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup, net_purchase),
 -- ── PORTFOLIO CLUSTER (source: sps_efficiency, AQS v7 methodology) ──────────
 -- Denominators
 se.sku_listed,       -- Universe of listed SKUs for the supplier. Denominator for % zero and % slow movers. Aligns with COUNT(DISTINCT sku WHERE is_listed) in sku_efficiency_detail_v2.
 se.sku_mature,       -- Listed SKUs with updated_sku_age >= 90 days (with reset logic). Denominator for efficiency %. Aligns with AQS v7 definition.
 se.sku_new,          -- Listed SKUs with updated_sku_age <= 30 days. Used for Newness % = sku_new / sku_listed. Aligns with assortment_quality_scorecard_v7.
 se.sku_probation,    -- Listed SKUs with updated_sku_age between 31-89 days. Used as part of efficient_listings denominator = (sku_new + sku_probation). Aligns with assortment_quality_scorecard_v7.

 -- Numerators (mature SKUs only)
 se.efficient_movers, -- Mature SKUs selling at or above category threshold. Retained for reference — AQS v7 calculates efficiency as 1-(zero+slow)/mature, not efficient/mature directly.

 -- Availability ingredients (never use new_availability directly in Tableau)
 se.numerator_new_avail, -- SUM(available_events_weightage * sales_forecast_qty_corr). Ingredient for weighted availability. Aditivo — agregar con SUM en cualquier nivel.
 se.denom_new_avail,     -- SUM(total_events_weightage * sales_forecast_qty_corr) with NULLIF(0). Ingredient for weighted availability. Tableau formula: SUM(numerator) / SUM(denom).

 -- Efficiency weighted ingredient (AQS v7 methodology)
 se.weight_efficiency, -- GPV-weighted efficiency numerator. Tableau: SUM(weight_efficiency)/SUM(gpv_eur)
 se.gpv_eur,           -- GPV denominator for weighted efficiency aggregation.

 -- EXCLUDED FIELDS (do not restore):
 -- la_zero_movers, la_slow_movers: low availability movers — supplier can blame stock, not useful for negotiation argument
 -- new_zero_movers, new_slow_movers, new_efficient_movers: non-mature SKUs — unfair to penalize supplier for SKUs still in ramp-up
 -- sold_items, gpv_eur: duplicated from sps_financial_metrics — use Net_Sales fields instead
 -- last_year_time_period: internal join key, not a metric
 -- new_availability: pre-calculated ratio — always use ingredients in Tableau to avoid aggregation errors
 listed.listed_skus,
 se.sku_listed AS listed_skus_efficiency,
 shrink.spoilage_value_eur,
 shrink.spoilage_value_lc,
 shrink.retail_revenue_eur,
 shrink.retail_revenue_lc,
 shrink.spoilage_rate,
 deliv.delivery_cost_eur,
 deliv.delivery_cost_local,
 po.on_time_orders,
 po.total_received_qty_per_po_order,
 po.total_demanded_qty_per_po_order,
 po.total_cancelled_po_orders,
 po.total_non_cancelled__po_orders,
 po.fill_rate,
 po.otd,
 po.supplier_non_fulfilled_order_qty,
 po.total_po_orders,
 po.total_compliant_po_orders,
 po.total_received_qty_ALL,
 po.total_demanded_qty_ALL,
 mc.total_market_customers,
 mc.total_market_orders
FROM all_keys AS o
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_price_index` AS p
  ON o.global_entity_id = p.global_entity_id AND o.time_period = p.time_period AND o.time_granularity = p.time_granularity AND o.division_type = p.division_type AND o.supplier_level = p.supplier_level AND o.entity_key = p.entity_key AND o.brand_sup = p.brand_sup
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_days_payable` AS dpo
  ON o.global_entity_id = dpo.global_entity_id AND o.time_period = dpo.time_period AND o.time_granularity = dpo.time_granularity AND o.division_type = dpo.division_type AND o.supplier_level = dpo.supplier_level AND o.entity_key = dpo.entity_key AND o.brand_sup = dpo.brand_sup
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_financial_metrics` AS sfm
  ON o.global_entity_id = sfm.global_entity_id AND o.time_period = sfm.time_period AND o.time_granularity = sfm.time_granularity AND o.division_type = sfm.division_type AND o.supplier_level = sfm.supplier_level AND o.entity_key = sfm.entity_key AND o.brand_sup = sfm.brand_sup
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_line_rebate_metrics` AS slrm
  ON o.global_entity_id = slrm.global_entity_id AND o.time_period = slrm.time_period AND o.time_granularity = slrm.time_granularity AND o.division_type = slrm.division_type AND o.supplier_level = slrm.supplier_level AND o.entity_key = slrm.entity_key AND o.brand_sup = slrm.brand_sup
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_efficiency` AS se
  ON o.global_entity_id = se.global_entity_id AND o.time_period = se.time_period AND o.time_granularity = se.time_granularity AND o.division_type = se.division_type AND o.supplier_level = se.supplier_level AND o.entity_key = se.entity_key AND o.brand_sup = se.brand_sup
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_listed_sku` AS listed
  ON o.global_entity_id = listed.global_entity_id AND o.time_period = listed.time_period AND o.time_granularity = listed.time_granularity AND o.division_type = listed.division_type AND o.supplier_level = listed.supplier_level AND o.entity_key = listed.entity_key AND o.brand_sup = listed.brand_sup
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_shrinkage` AS shrink
  ON o.global_entity_id = shrink.global_entity_id AND o.time_period = shrink.time_period AND o.time_granularity = shrink.time_granularity AND o.division_type = shrink.division_type AND o.supplier_level = shrink.supplier_level AND o.entity_key = shrink.entity_key AND o.brand_sup = shrink.brand_sup
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_delivery_costs` AS deliv
  ON o.global_entity_id = deliv.global_entity_id AND o.time_period = deliv.time_period AND o.time_granularity = deliv.time_granularity AND o.division_type = deliv.division_type AND o.supplier_level = deliv.supplier_level AND o.entity_key = deliv.entity_key AND o.brand_sup = deliv.brand_sup
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_purchase_order` AS po
  ON o.global_entity_id = po.global_entity_id AND o.time_period = po.time_period AND o.time_granularity = po.time_granularity AND o.division_type = po.division_type AND o.supplier_level = po.supplier_level AND o.entity_key = po.entity_key AND o.brand_sup = po.brand_sup
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_market_customers` AS mc
  ON o.global_entity_id = mc.global_entity_id AND o.time_period = mc.time_period AND o.time_granularity = mc.time_granularity
;
