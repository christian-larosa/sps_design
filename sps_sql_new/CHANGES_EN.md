# SPS SQL New | Modified Files

This folder contains the original files (sps_originals) with critical documented changes applied. **Only 8 files were modified**, the rest are copies without changes.

## 🔴 Files to Review (8 total)

### Tier 1: Critical (Review first)

#### 1. **sps_efficiency_month.sql**
- **Main change:** AQS v5 → AQS v2/v7
- **Source:** `_aqs_v5_sku_efficiency_detail` → `sku_efficiency_detail_v2`
- **Removed fields:** date_diff, avg_qty_sold, new_availability
- **New fields (8):** sku_efficiency (ENUM), updated_sku_age, available_hours, potential_hours, numerator_new_avail, denom_new_avail, sku_status, is_listed
- **Impact:** Everything downstream depends on this

#### 2. **sps_efficiency.sql**
- **Main change:** Refactor from 1 CTE → 3 CTEs (sku_counts, efficiency_by_warehouse, combined)
- **New metric:** weight_efficiency (AQS v7 methodology = efficient_skus / qualified_skus * gpv_eur)
- **New ingredients:** numerator_new_avail, denom_new_avail, weight_efficiency
- **Impact:** Feeds sps_score_tableau final columns

#### 3. **sps_score_tableau.sql**
- **Main change:** Architecture from sequential JOINs → all_keys UNION pattern
- **Why:** Ensures coverage of ALL key combinations (global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity) that exist in any of the 8 source tables
- **Impact:** FINAL TABLE - coverage/completeness change

### Tier 2: Important (Review second)

#### 4. **sps_price_index.sql**
- **New ingredients:** price_index_numerator, price_index_weight
- **Tableau formula:** median_price_index = SUM(price_index_numerator) / SUM(price_index_weight)

#### 5. **sps_days_payable.sql**
- **New ingredients:** stock_value_eur, cogs_monthly_eur, days_in_month, days_in_quarter
- **Tableau formula:** doh_monthly = SUM(stock_value_eur) / (SUM(cogs_monthly_eur) / MAX(days_in_month))

#### 6. **sps_shrinkage.sql**
- **Changes:** Field renames (spoilage_value → spoilage_value_eur, retail_revenue → retail_revenue_eur)
- **New:** spoilage_value_lc, retail_revenue_lc (multi-currency support)

#### 7. **sps_line_rebate_metrics.sql**
- **New ingredients:** calc_net_delivered, calc_net_return, and calc_gross two fields (2)
- **Base for:** Rebate calculations in Tableau (net_purchase = SUM(calc_net_delivered) - SUM(calc_net_return))

### Tier 3: Debug (Review for validation)

#### 8. **sps_purchase_order.sql**
- **New debug fields:** total_po_orders, total_compliant_po_orders, total_received_qty_ALL, total_demanded_qty_ALL
- **Use:** Validation and debugging of aggregations

## 📋 Unchanged Files (do not review)

The following files are direct copies without modifications:
- sps_product.sql
- sps_customer_order.sql
- sps_delivery_costs.sql
- sps_days_payable_month.sql (hardcoding changes only)
- sps_delivery_costs_month.sql (hardcoding changes only)
- sps_financial_metrics.sql
- sps_financial_metrics_month.sql
- sps_financial_metrics_prev_year.sql
- sps_listed_sku.sql
- sps_listed_sku_month.sql
- sps_line_rebate_metrics_month.sql (hardcoding changes only)
- sps_price_index_month.sql (hardcoding changes only)
- sps_product.sql
- sps_purchase_order_month.sql (hardcoding changes only)
- sps_shrinkage_month.sql (hardcoding changes only)

## ✅ Recommendation for Review Order

1. **First:** Read sps_efficiency_month.sql (understand AQS v5→v7 change)
2. **Second:** Read sps_efficiency.sql (understand 3 CTEs + weight_efficiency)
3. **Third:** Read sps_score_tableau.sql (understand all_keys UNION)
4. **Then:** Review sps_price_index, sps_days_payable, sps_shrinkage, sps_line_rebate_metrics (ingredients)
5. **Finally:** sps_purchase_order (debug fields)

**Note:** All changes have inline comments `-- NEW` or `-- MODIFIED` for easy follow-up.
