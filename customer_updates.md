# SPS Customer Updates — 2026-04-23

## Summary
Three script updates to fix financial metrics deduplication and add market-level customer metrics to the SPS scorecard platform.

---

## 1. flat_sps_financial_metrics.sql ✅

**Status:** Fixed & Validated

### Problem
Fields `amt_total_price_paid_net_lc`, `amt_total_price_paid_net_eur`, and `amt_gbv_eur` are basket-level values but live in `sps_financial_metrics_month` at grain `order_id × sku_id`. This caused a fan-out: each SKU of a multi-SKU order repeated the basket value.

**Impact:** 
- Supplier 258, Nov 2025: inflation_factor = 2.3x in 9,496 orders (Discovery confirmed)
- Total_Net_Sales_lc_order was inflated by 63%

### Solution
Subquery with ROW_NUMBER() window function:
- Partitions by `order_id + supplier_id + global_entity_id + month`
- Only ROW_NUMBER() = 1 receives the actual basket value
- Remaining rows receive 0 → SUM() adds value once per order
- Deterministic ORDER BY sku_id for reproducibility

### Changes
- Added subquery (lines 73-99) with CASE WHEN ROW_NUMBER() = 1
- Updated three SELECT fields to reference `*_dedup` columns:
  - `SUM(amt_total_price_paid_net_eur_dedup)` → Total_Net_Sales_eur_order
  - `SUM(amt_total_price_paid_net_lc_dedup)` → Total_Net_Sales_lc_order
  - `SUM(amt_gbv_eur_dedup)` → total_GBV

### Validation
**Before:** ABV = 102.01 LC (inflated)  
**After:** ABV = 62.38 LC (correct ✓)  
**Reference:** sps_customer_order actual basket average = 62.38 LC  
**Match:** ✓ EXACT

---

## 2. flat_sps_market_customers.sql (NEW) ✅

**Status:** Created & Validated

### Purpose
Denominator table for `customer_penetration` metric in customer segmentation analysis.

### Specification
- **Grain:** `global_entity_id × time_period × time_granularity`
- **Coverage:** All customers on platform (no supplier filter)
- **Metrics:**
  - `total_market_customers`: COUNT(DISTINCT analytical_customer_id)
  - `total_market_orders`: COUNT(DISTINCT order_id)
- **Granularities:**
  - **Monthly:** Unique customers per calendar month
  - **Quarterly:** Unique customers over entire quarter (not sum of months)

### Key Logic
Quarterly COUNT DISTINCT is computed over full quarter period, not aggregated from monthly counts. Example: A customer purchasing in Oct + Nov + Dec = 1 quarterly customer, not 3.

### Data
**Period:** 2025-10-01 to current date  
**Region:** Peru (country_code = 'pe')  
**Rows:** 6 (3 months × 2 granularities)

**Sample:**
| Month | Quarterly | Monthly Customers | Quarterly Customers |
|-------|-----------|-------------------|---------------------|
| Oct 2025 | Q4-2025 | 69,682 | 145,170 |
| Nov 2025 | Q4-2025 | 74,118 | 145,170 |
| Dec 2025 | Q4-2025 | 83,862 | 145,170 |

### Validation
Sanity check: `quarterly_customers ≥ monthly_customers` for all rows  
**Result:** All rows = 'OK' ✓

---

## 3. flat_sps_score_tableau.sql ✅

**Status:** Updated & Executed

### Changes
Added market customers context to the main SPS scorecard table.

**JOIN (lines 124-125):**
```sql
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_market_customers` AS mc
  ON o.global_entity_id = mc.global_entity_id
  AND o.time_period = mc.time_period
  AND o.time_granularity = mc.time_granularity
```

**Fields (lines 103-104):**
- `mc.total_market_customers`
- `mc.total_market_orders`

### Purpose
Enables Tableau to calculate:
- Customer penetration: `supplier_customers / total_market_customers`
- Market share by customer count
- Growth trends relative to total platform activity

---

## Execution Order

1. ✅ flat_sps_financial_metrics.sql — Fixed basket-level deduplication
2. ✅ flat_sps_market_customers.sql — Created market-level denominators
3. ✅ flat_sps_score_tableau.sql — Integrated market context into scorecard

---

## Files Modified

| File | Status | Location |
|------|--------|----------|
| flat_sps_financial_metrics.sql | ✅ Updated | `/sps_sql_prod/` |
| flat_sps_market_customers.sql | ✅ Created | `/sps_sql_prod/` |
| flat_sps_score_tableau.sql | ✅ Updated | `/sps_sql_prod/` |

---

## 4. flat_sps_supplier_segmentation.sql — Architecture Refactor ✅

**Status:** Updated (Self-Contained)

### Problem
The segmentation table required external JOINs to sps_score_tableau to access essential financial and customer metrics (total_customers, total_orders, Net_Sales_eur, Net_Sales_lc), defeating the purpose of having a dedicated segmentation table.

### Solution
Added five context fields to the segmentation table's SELECT list:
- `total_customers` — Unique supplier customers per period
- `total_orders` — Total orders placed from supplier per period
- `total_market_customers` — Platform-wide customer count (for penetration denominator)
- `Net_Sales_eur` — Gross sales in EUR
- `Net_Sales_lc` — Gross sales in local currency

### Changes
**Propagated through all CTEs:**
1. **base** (lines 68-74): Added five fields to SELECT
2. **percentiles** (lines 125-131): Pass through in SELECT
3. **scoring** (lines 187-192): Pass through as `b.field_name`
4. **final** (lines 242-247): Include in final SELECT output

### Impact
- Segmentation table is now **self-contained** — no external JOIN required
- Analysts can analyze segment composition with financial context directly
- Enables direct Tableau dashboard from sps_supplier_segmentation (no denormalization step)

---

## Next Steps

- Monitor sps_supplier_segmentation in Tableau for segment composition and financial patterns
- Validate no regressions in existing supplier-level metrics
- Document segment thresholds in data dictionary (importance > 15, productivity >= 40)
- Monitor top-decile Key Accounts for growth and penetration trends

