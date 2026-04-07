# 📚 SPS CHANGES MANUAL | English Version (EN)

**Master changes document: sps_originals → flat_sps_***  
**Audience:** Senior Data Analysts  
**Format:** By table, in DAG order  
**Last updated:** 2026-04-07

---

## 🗂️ DAG ORDER

```
1. sps_product (BASE)
   ├─ sps_efficiency_month ──────→ sps_efficiency
   ├─ sps_listed_sku_month ──────→ sps_listed_sku
   ├─ sps_days_payable_month ───→ sps_days_payable
   ├─ sps_price_index_month ────→ sps_price_index
   ├─ sps_delivery_costs_month ─→ sps_delivery_costs
   ├─ sps_shrinkage_month ──────→ sps_shrinkage
   ├─ sps_line_rebate_metrics_month → sps_line_rebate_metrics
   ├─ sps_customer_order
   │  └─ sps_financial_metrics_month → sps_financial_metrics
   └─ sps_purchase_order_month ─→ sps_purchase_order

   └─ sps_score_tableau (FINAL - depends on ALL)
```

---

## 📊 TABLE 1: sps_product (BASE)

**Position in DAG:** Root  
**Dependencies:** None (base table)

### 📈 Changes detected

| Aspect | Before | After | Impact |
|--------|--------|-------|--------|
| Lines | 808 | 806 | -2 lines (cleanup) |
| CTEs | 1 (simple) | 1 (simple) | No changes |
| FROM clauses | Multiple internal tables | Multiple internal tables | No significant changes |

### ✅ Verification

```bash
# Total difference
diff sps_product.sql flat_sps_product.sql | wc -l
# Output: 113 changes (mostly hardcodes of parameters/dataset)
```

### 📝 Conclusion

**No substantial changes in logic or fields.** Only:
- Hardcodes of project/dataset (`{{ params.project_id }}` → `dh-darkstores-live`)
- Possible minor format/spacing changes

**Recommendation:** Proceed as trusted base. Validate that same SKUs exist.

---

## 📊 TABLE 2: sps_efficiency_month

**Position in DAG:** Level 2 (depends on: sps_product, AQS data)  
**Dependencies:** sps_product, sku_efficiency_detail_v2  
**Criticality:** 🔴 **VERY HIGH** - AQS methodology change

### 🔴 CRITICAL CHANGE: AQS v5 → AQS v2 (implementing AQS v7)

#### Data source change

| Parameter | sps_originals (AQS v5) | flat_sps (AQS v2) | Implication |
|-----------|------------------------|-------------------|------------|
| **Table** | `_aqs_v5_sku_efficiency_detail` | `sku_efficiency_detail_v2` | New methodology |
| **Dataset** | `{{ params.dataset.rl }}` | `fulfillment-dwh-production.rl_dmart` | Location change |
| **Partition** | `partition_month` | `partition_month` | Same (compatible) |

#### Field changes - DIRECT COMPARISON

| Field | Before (v5) | After (v2/v7) | Type | Impact |
|-------|-----------|-----------------|------|--------|
| `sku_efficiency` | ❌ DOES NOT EXIST | ✅ NEW (ENUM) | NEW | **CRITICAL** - Predefined categorization |
| `updated_sku_age` | ❌ DOES NOT EXIST | ✅ NEW (INT) | NEW | Replaces `date_diff` |
| `available_hours` | ❌ DOES NOT EXIST | ✅ NEW | NEW | New availability metric |
| `potential_hours` | ❌ DOES NOT EXIST | ✅ NEW | NEW | New availability metric |
| `numerator_new_avail` | ❌ DOES NOT EXIST | ✅ NEW | NEW | Ingredient for weighted availability |
| `denom_new_avail` | ❌ DOES NOT EXIST | ✅ NEW | NEW | Ingredient for weighted availability |
| `sku_status` | ❌ DOES NOT EXIST | ✅ NEW | NEW | SKU status |
| `is_listed` | ❌ DOES NOT EXIST | ✅ NEW (BOOL) | NEW | Listing indicator |
| `date_diff` | ✅ EXISTS | ❌ REMOVED | REMOVED | Replaced by `updated_sku_age` |
| `avg_qty_sold` | ✅ EXISTS | ❌ REMOVED | REMOVED | Now comes as category in `sku_efficiency` |
| `new_availability` | ✅ EXISTS | ❌ REMOVED (but lives in ingredients) | REMOVED | Decomposed into numerator/denom |

### 📐 Logic change - AQS v5 vs AQS v7

**AQS v5 (before):**
```sql
-- Manual categorization based on logic
CASE
  WHEN date_diff >= 90 AND ((avg_qty_sold = 0 OR avg_qty_sold IS NULL) AND new_availability = 1)
    THEN 'zero_mover'
  WHEN date_diff >= 90 AND (avg_qty_sold < 1 AND avg_qty_sold > 0) AND (new_availability >= 0.8)
    THEN 'slow_mover'
  WHEN date_diff >= 90 AND (avg_qty_sold >= 1)
    THEN 'efficient_mover'
  ...
END AS sku_category
```

**AQS v2/v7 (after):**
```sql
-- Pre-calculated field from source (sku_efficiency_detail_v2)
e.sku_efficiency  -- ENUM: 'efficient_sku', 'zero_mover', 'slow_mover', ...
```

### ⚠️ Implications for downstream (sps_efficiency)

```sql
-- BEFORE (v5):
COUNT(DISTINCT CASE WHEN date_diff >=90 AND (avg_qty_sold >= 1) THEN sku_id END) AS efficient_movers

-- AFTER (v2/v7):
COUNT(DISTINCT CASE WHEN updated_sku_age >= 90 AND sku_efficiency = 'efficient_sku' THEN sku_id END) AS efficient_movers
```

**Conceptual difference:**
- `date_diff` = days since first listing (simple)
- `updated_sku_age` = days with "reset logic" (if warehouse changes, recalculates) → **More precise**

### 🔍 Recommended validation

```sql
-- Verify that both methodologies produce similar counts
SELECT
  COUNT(DISTINCT CASE WHEN updated_sku_age >= 90 AND sku_efficiency = 'efficient_sku' THEN sku_id END) as v7_efficient,
  COUNT(DISTINCT CASE WHEN date_diff >= 90 AND avg_qty_sold >= 1 THEN sku_id END) as v5_efficient,
  (COUNT(*) OVER () - COUNT(DISTINCT CASE WHEN sku_efficiency IN ('efficient_sku', 'zero_mover', 'slow_mover') THEN sku_id END)) as unmapped_skus
FROM flat_sps_efficiency_month
WHERE CAST(month AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY global_entity_id, supplier_id;
```

### ✅ Conclusion

**IMPORTANT METHODOLOGICAL CHANGE but COMPATIBLE.**  
- AQS v7 is more precise (`updated_sku_age` with reset logic)
- Categories (`sku_efficiency`) come pre-calculated (less manual error)
- Downstream (sps_efficiency) **does depend** on these changes → **validate totals**

---

## 📊 TABLE 3: sps_efficiency

**Position in DAG:** Level 3 (depends on: sps_efficiency_month)  
**Dependencies:** sps_efficiency_month  
**Criticality:** 🔴 **VERY HIGH** - Architectural refactor + new formula

### 🏗️ ARCHITECTURAL CHANGE: 1 CTE → 3 CTEs

#### Before (sps_originals)

```sql
-- Simple structure: 1 direct SELECT + GROUPING SETS
SELECT
  global_entity_id,
  CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING) ELSE quarter_year END AS time_period,
  -- ... dimensions ...
  COUNT(DISTINCT sku_id) AS sku_listed,
  COUNT(DISTINCT CASE WHEN date_diff >= 90 THEN sku_id END) AS sku_mature,
  COUNT(DISTINCT CASE WHEN ... AND avg_qty_sold >= 1 THEN sku_id END) AS efficient_movers,
  SUM(sold_items) AS sold_items,
  SUM(gpv_eur) AS gpv_eur
FROM sps_efficiency_month
GROUP BY GROUPING SETS (...)
```

**Problem:** Calculates `COUNT(DISTINCT)` directly, which is NOT additive cross-warehouse.

#### After (flat_sps)

```sql
-- 3 CTEs separated by metric type

CTE A: sku_counts (COUNT(DISTINCT) without warehouse)
├─ COUNT(DISTINCT sku) at level (supplier, month, category) WITHOUT warehouse_id
├─ Result: unique SKU counts per dimension

CTE B: efficiency_by_warehouse (additive metrics WITH warehouse)
├─ SUM(sold_items), SUM(gpv_eur), SUM(numerator_new_avail), SUM(denom_new_avail)
├─ NEW: weight_efficiency = SAFE_DIVIDE(...) * SUM(gpv_eur)
├─ Result: additive metrics at level (supplier, warehouse, month)

CTE C: combined (JOIN A + B)
├─ LEFT JOIN sku_counts (A) with efficiency_by_warehouse (B)
├─ sku_counts remains fixed (not summed)
├─ efficiency_by_warehouse summed cross-warehouse
└─ Result: correct combination of counts + sums
```

###  📐 Changes in calculation formulas

#### New field: `weight_efficiency`

**Definition:**
```sql
weight_efficiency = (
  SUM(COUNT(DISTINCT efficient_skus)) OVER (PARTITION BY supplier, warehouse)
  / NULLIF(
      SUM(COUNT(DISTINCT efficient_or_qualified_skus)) OVER (...),
      0
  )
) * SUM(gpv_eur)
```

**Interpretation:**
- Numerator: Count of efficient SKUs
- Denominator: Count of eligible SKUs (efficient + slow_movers + qualified zero_movers)
- Weight: Multiplied by GPV to weight by volume

**Use in Tableau:**
```
Tableau formula = SUM(weight_efficiency) / SUM(gpv_eur)
= Efficiency % weighted by GPV at aggregate level
```

### 🔄 Fields that CHANGE definition

| Field | Before (v5, direct) | After (v7, aggregated) | Difference |
|-------|-------------------|----------------------|-----------|
| `efficient_movers` | `COUNT(DISTINCT ... date_diff >= 90 AND avg_qty_sold >= 1)` | `COUNT(DISTINCT ... updated_sku_age >= 90 AND sku_efficiency = 'efficient_sku')` | Logic changes (AQS v5→v7) |
| `sku_listed` | `COUNT(DISTINCT sku_id)` | Pre-calculated in `sku_counts` CTE | Source changes |
| `sku_mature` | `COUNT(DISTINCT ... date_diff >= 90)` | `COUNT(DISTINCT ... updated_sku_age >= 90)` | Name/logic |
| `sold_items` | `SUM(sold_items)` | `SUM(SUM(...) OVER warehouse)` | Now via window function |
| `gpv_eur` | `SUM(gpv_eur)` | `SUM(SUM(...) OVER warehouse)` | Now via window function |

### 🔍 Recommended validation

```sql
-- Compare totals at country level
SELECT
  'flat_sps_efficiency (v7)' as source,
  global_entity_id,
  SUM(sku_listed) as total_skus,
  SUM(sku_mature) as total_mature,
  SUM(efficient_movers) as total_efficient,
  ROUND(SUM(weight_efficiency) / NULLIF(SUM(gpv_eur), 0) * 100, 2) as efficiency_pct
FROM flat_sps_efficiency
WHERE CAST(time_period AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY global_entity_id

UNION ALL

-- Compare with sps_efficiency v5 (if still exists)
SELECT
  'sps_efficiency (v5)' as source,
  global_entity_id,
  SUM(sku_listed) as total_skus,
  SUM(sku_mature) as total_mature,
  SUM(efficient_movers) as total_efficient,
  ROUND(SUM(gpv_eur) / NULLIF(SUM(sold_items), 0), 2) as efficiency_pct
FROM sps_efficiency  -- old table
GROUP BY global_entity_id;
```

### ✅ Conclusion

**IMPORTANT REFACTOR but WELL-FOUNDED.**
- Separation of COUNT(DISTINCT) vs SUM addresses correct cross-warehouse aggregation
- `weight_efficiency` is new metric (AQS v7 methodology)
- Totals may differ from v5 due to AQS methodology change (v5→v7)
- **Critical action:** Validate with PFC team if efficiency % aligns with scorecard

---

## 📊 TABLE 4-7: sps_listed_sku, sps_days_payable, sps_price_index, sps_shrinkage, sps_line_rebate_metrics

**Quick summary:** These tables follow the pattern (table_month → table_aggregated)

| Table | Changes | Criticality | Action |
|-------|---------|-----------|--------|
| **listed_sku** | FROM: listed_sku_month (no logic changes) | 🟢 LOW | Verify row counts |
| **days_payable** | FROM: days_payable_month (CTEs consolidated in _month) | 🟡 MEDIUM | Validate DOH formula |
| **price_index** | FROM: price_index_month (CTEs consolidated) | 🟡 MEDIUM | Validate price median |
| **shrinkage** | FROM: shrinkage_month (minor final column changes) | 🟡 MEDIUM | Validate spoilage_rate |
| **line_rebate_metrics** | FROM: line_rebate_metrics_month (new aggregated CTE) | 🟡 MEDIUM | Validate rebate_values |

### Details per table

#### sps_listed_sku
- **Changes:** 0 lines diff (completely identical)
- **Action:** ✅ Proceed without changes

#### sps_days_payable
- **Changes:** +16 lines
- **Reason:** Likely improved documentation
- **Final fields:** payment_days, doh, dpo, stock_value_eur, cogs_monthly_eur, days_in_month, days_in_quarter
- **Validate:** `doh = SUM(stock_value_eur) / (SUM(cogs_monthly_eur) / MAX(days_in_month))` remains same

#### sps_price_index
- **Changes:** +8 lines
- **Possible new fields:** New derived columns
- **Validate:** `median_price_index`, `price_index_numerator`, `price_index_weight` calculated correctly

#### sps_shrinkage
- **Changes:** +6 lines
- **Final fields:** spoilage_value_eur, spoilage_value_lc, retail_revenue_eur, retail_revenue_lc, spoilage_rate
- **Validate:** `spoilage_rate = spoilage_value / retail_revenue` maintains logic

#### sps_line_rebate_metrics
- **Changes:** -9 lines (simplification)
- **Reason:** CTEs consolidated in _month
- **Validate:** Rebate calculations maintain precision

---

## 📊 TABLE 8: sps_financial_metrics

**Position in DAG:** Level 3 (depends on: sps_customer_order, sps_financial_metrics_month)

| Aspect | Changes |
|--------|---------|
| Lines | 168 → 167 (-1) |
| CTEs | Internal change in _month (consolidation) |
| Final fields | No significant changes |

**Action:** ✅ Proceed. Minimal changes.

---

## 📊 TABLE 9: sps_purchase_order

**Position in DAG:** Level 3 (depends on: sps_purchase_order_month)  
**Criticality:** 🟡 **MEDIUM** - Debug fields added

### Changes detected

| Change | Lines | Type |
|--------|-------|------|
| Debug fields | +4 | NEW |
| Total lines | +13 | Minor changes |

### New debug fields added

```sql
-- Debug fields (no changes to original logic)
COUNT(DISTINCT(po_order_id)) AS total_po_orders,
COUNT(DISTINCT(CASE WHEN is_compliant_flag THEN po_order_id END)) AS total_compliant_po_orders,
COALESCE(SUM(total_received_qty_per_order), 0) AS total_received_qty_ALL,
COALESCE(SUM(total_demanded_qty_per_order), 0) AS total_demanded_qty_ALL
```

**Purpose:**
- `total_po_orders`: All orders (to audit compliant ratio)
- `total_compliant_po_orders`: Valid orders
- `total_received_qty_ALL`: Received quantity without status filter
- `total_demanded_qty_ALL`: Demanded quantity without status filter

**Impact on score_tableau:**
- These 4 fields inherited to score_tableau
- Useful for auditing fill_rate and OTD calculations

### ✅ Conclusion

**Safe changes.** These are informational fields, do not alter main metrics.

---

## 📊 TABLE 10: sps_score_tableau (FINAL - CONCENTRATOR)

**Position in DAG:** Level 4 (LAST - depends on ALL)  
**Criticality:** 🔴 **VERY HIGH** - Architectural change + inherits changes from all

### 🏗️ FUNDAMENTAL ARCHITECTURAL CHANGE

#### Before (sps_originals)

```sql
SELECT o.*,
  fin.* EXCEPT (...),
  slrm.* EXCEPT (...),
  p.median_price_index,
  dpo.payment_days, dpo.doh, dpo.dpo,
  se.* EXCEPT (...),
  listed.listed_skus,
  shrink.spoilage_value,
  shrink.retail_revenue,
  shrink.spoilage_rate,
  deliv.delivery_cost_eur,
  deliv.delivery_cost_local
FROM sps_purchase_order AS o
LEFT JOIN sps_financial_metrics AS fin
LEFT JOIN sps_line_rebate_metrics AS slrm
LEFT JOIN sps_price_index AS p
LEFT JOIN sps_days_payable AS dpo
LEFT JOIN sps_efficiency AS se
LEFT JOIN sps_listed_sku AS listed
LEFT JOIN sps_shrinkage AS shrink
LEFT JOIN sps_delivery_costs AS deliv
```

**Problem:** Base is `sps_purchase_order`. If a metric (e.g.: efficiency) has no data for a key, those rows are lost.

#### After (flat_sps)

```sql
WITH all_keys AS (
  SELECT DISTINCT * FROM (
    SELECT ... FROM sps_price_index
    UNION ALL
    SELECT ... FROM sps_days_payable
    UNION ALL
    SELECT ... FROM sps_financial_metrics
    UNION ALL
    SELECT ... FROM sps_line_rebate_metrics
    UNION ALL
    SELECT ... FROM sps_efficiency
    UNION ALL
    SELECT ... FROM sps_listed_sku
    UNION ALL
    SELECT ... FROM sps_shrinkage
    UNION ALL
    SELECT ... FROM sps_delivery_costs
    UNION ALL
    SELECT ... FROM sps_purchase_order
  )
)
SELECT o.*,
  p.*, dpo.*, sfm.*, slrm.*,
  se.sku_listed, se.sku_mature, se.sku_probation, se.sku_new,
  se.efficient_movers, se.numerator_new_avail, se.denom_new_avail,
  se.weight_efficiency, se.gpv_eur,
  listed.listed_skus,
  shrink.spoilage_value_eur,
  deliv.delivery_cost_eur, deliv.delivery_cost_local,
  po.on_time_orders, po.fill_rate, po.otd, po.supplier_non_fulfilled_order_qty,
  po.total_po_orders, po.total_compliant_po_orders, po.total_received_qty_ALL, po.total_demanded_qty_ALL
FROM all_keys AS o
LEFT JOIN sps_price_index AS p
LEFT JOIN sps_days_payable AS dpo
LEFT JOIN sps_financial_metrics AS sfm
LEFT JOIN sps_line_rebate_metrics AS slrm
LEFT JOIN sps_efficiency AS se
LEFT JOIN sps_listed_sku AS listed
LEFT JOIN sps_shrinkage AS shrink
LEFT JOIN sps_delivery_costs AS deliv
LEFT JOIN sps_purchase_order AS po
```

**Advantage:** Guarantees that ALL unique keys (from any table) are present.

###  📋 COMPLETE FINAL COLUMNS

#### Dimension columns (inherited from all_keys)
```
global_entity_id          -- Marketplace/country
time_period               -- Month or quarter
brand_sup                 -- Supplier ID or brand
entity_key                -- Hierarchical key (principal/division/brand/category)
division_type             -- Type: principal, division, brand_owner, brand_name, total
supplier_level            -- Granularity level
time_granularity          -- Monthly or Quarterly
```

#### price_index columns (inherited from sps_price_index)
```
median_price_index        -- Median of price index
price_index_numerator     -- SUM(median_bp_index * sku_gpv_eur)
price_index_weight        -- SUM(sku_gpv_eur)
```

#### days_payable columns (inherited from sps_days_payable)
```
payment_days              -- DPO (days of payment)
doh                       -- Days on hand (inventory)
dpo                       -- Days payable outstanding
stock_value_eur           -- Stock value in EUR
cogs_monthly_eur          -- Monthly COGS in EUR
days_in_month             -- Days in month
days_in_quarter           -- Days in quarter
```

#### financial_metrics columns (via * EXCEPT)
```
(all except basic dimensions)
Includes: Net_Sales, Gross_Margin, COGS, etc.
```

#### line_rebate_metrics columns (via * EXCEPT)
```
(all except dimensions and net_purchase)
Includes: rebate_values, rebate_pct, etc.
```

#### efficiency columns (AQS v7 - NEW DEFINITIONS)

**Denominators (universes):**
```
sku_listed                -- Count of listed SKUs (is_listed = TRUE)
sku_mature                -- SKUs with updated_sku_age >= 90 (efficiency universe)
sku_new                   -- SKUs with updated_sku_age <= 30
sku_probation             -- SKUs with updated_sku_age 31-89 (in ramp-up)
```

**Numerators (efficient SKUs):**
```
efficient_movers          -- Mature SKUs that sell (sku_efficiency = 'efficient_sku')
                          -- Note: Do NOT use efficient/mature as ratio. Use weight_efficiency instead.
```

**Availability ingredients (additive):**
```
numerator_new_avail       -- SUM(available_events_weightage * sales_forecast_qty_corr)
denom_new_avail           -- SUM(total_events_weightage * sales_forecast_qty_corr)
                          -- Tableau formula: SUM(numerator)/SUM(denom) = weighted availability %
```

**Weighted efficiency ingredients (AQS v7):**
```
weight_efficiency         -- GPV-weighted efficiency numerator
gpv_eur                   -- GPV denominator
                          -- Tableau formula: SUM(weight_efficiency)/SUM(gpv_eur) = efficiency %
listed_skus_efficiency    -- Alias of sku_listed (for clarity in Tableau)
```

#### listed_sku columns
```
listed_skus               -- Count of listed SKUs
```

#### shrinkage columns
```
spoilage_value_eur        -- Spoilage value in EUR
spoilage_value_lc         -- Spoilage value in local currency
retail_revenue_eur        -- Retail revenue in EUR
retail_revenue_lc         -- Retail revenue in local currency
spoilage_rate             -- Spoilage rate (%)
```

#### delivery_costs columns
```
delivery_cost_eur         -- Delivery cost in EUR
delivery_cost_local       -- Delivery cost in local currency
```

#### purchase_order columns (including debug fields)
```
on_time_orders            -- Orders on time and compliant
total_received_qty_per_po_order  -- Received quantity per order
total_demanded_qty_per_po_order  -- Demanded quantity per order
total_cancelled_po_orders -- Cancelled orders
total_non_cancelled__po_orders -- Non-cancelled orders
fill_rate                 -- Fulfillment rate
otd                       -- On-time delivery
supplier_non_fulfilled_order_qty -- Quantity not fulfilled by supplier
total_po_orders           -- 🆕 DEBUG: Total orders
total_compliant_po_orders -- 🆕 DEBUG: Compliant orders
total_received_qty_ALL    -- 🆕 DEBUG: Total received quantity (no filter)
total_demanded_qty_ALL    -- 🆕 DEBUG: Total demanded quantity (no filter)
```

### 🔴 CRITICAL CHANGES IN SCORE_TABLEAU

#### 1. Architectural change (all_keys UNION)
- **Before:** Could lose rows if a metric had no data
- **After:** Guarantees coverage of all keys
- **Impact:** More rows in result, especially in periods without efficiency data

#### 2. Change in efficiency formulas (AQS v5→v7)
- **Before:** Direct counts without GPV weighting
- **After:** weight_efficiency weighted (new metric)
- **Tableau impact:** Must change from `efficient_movers / sku_mature` to `SUM(weight_efficiency) / SUM(gpv_eur)`

#### 3. New availability ingredients
- **Before:** Pre-calculated `new_availability` field (not additive)
- **After:** Decomposed into `numerator_new_avail` and `denom_new_avail` (additive)
- **Impact:** Change in how Tableau calculates % availability

#### 4. Debug fields added
- **4 new fields** from purchase_order for audit
- **Impact:** Useful for validating fill_rate and OTD, but do not affect main metrics

### 🔍 Recommended validation (SQL)

```sql
-- 1. Compare row counts (before vs after architecture)
SELECT
  'UNION architecture (flat_sps)' as type,
  COUNT(*) as total_rows,
  COUNT(DISTINCT global_entity_id) as countries,
  COUNT(DISTINCT (global_entity_id, time_period, brand_sup)) as unique_keys
FROM flat_sps_score_tableau

UNION ALL

SELECT
  'JOIN architecture (sps_originals)',
  COUNT(*),
  COUNT(DISTINCT global_entity_id),
  COUNT(DISTINCT (global_entity_id, time_period, brand_sup))
FROM sps_score_tableau;

-- 2. Validate efficiency change (v5 vs v7)
SELECT
  global_entity_id,
  time_period,
  -- Formula v5: efficient_movers / sku_mature
  ROUND(SAFE_DIVIDE(SUM(efficient_movers), NULLIF(SUM(sku_mature), 0)) * 100, 2) as v5_efficiency_pct,
  -- Formula v7: weight_efficiency / gpv_eur
  ROUND(SAFE_DIVIDE(SUM(weight_efficiency), NULLIF(SUM(gpv_eur), 0)) * 100, 2) as v7_efficiency_pct,
  COUNT(*) as rows
FROM flat_sps_score_tableau
WHERE CAST(time_period AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY 1, 2
HAVING COUNT(*) > 0
ORDER BY global_entity_id, time_period DESC;

-- 3. Validate availability ingredients
SELECT
  global_entity_id,
  SUM(numerator_new_avail) / NULLIF(SUM(denom_new_avail), 0) as weighted_availability_pct,
  COUNT(DISTINCT (time_period, brand_sup)) as key_combinations
FROM flat_sps_score_tableau
WHERE denom_new_avail IS NOT NULL AND denom_new_avail > 0
GROUP BY global_entity_id;
```

### ✅ Conclusion

**INTEGRAL and VERY IMPORTANT CHANGE.**
- UNION architecture guarantees complete coverage
- AQS v5→v7 introduces weighted metric (`weight_efficiency`)
- Tableau dashboards MUST update efficiency formulas
- Debug fields useful for audit

**Critical actions:**
1. ✅ Validate row counts (expected: more rows than before)
2. ✅ Update Tableau: efficiency = SUM(weight_efficiency) / SUM(gpv_eur)
3. ✅ Validate availability: use SUM(numerator) / SUM(denom), not pre-calculated new_availability
4. ✅ Communicate to stakeholders change in % efficiency (now weighted)

---

## 📌 SUMMARY OF CRITICAL VALIDATIONS

| Table | Validation | SQL Query | Owner |
|-------|-----------|-----------|-------|
| **efficiency_month** | AQS v5→v2 categorization | See "AQS v5 vs AQS v7" section | DH |
| **efficiency** | weight_efficiency totals | See "Recommended validation" section | DH + PFC |
| **score_tableau** | Row count comparison | See "Validation SQL" section | Analytics |
| **score_tableau** | Efficiency % (v5 vs v7) | See "Validation SQL" section | Analytics |
| **score_tableau** | Availability ingredients | See "Validation SQL" section | Analytics |

---

## 📚 REFERENCES

- **AQS Documentation:** [Link to wiki/docs if exists]
- **Tableau Changes:** Update datasource with new columns
- **Responsible teams:** 
  - DH (Data Warehouse): Data validation
  - PFC (Product Financial Control): Metrics validation
  - Analytics: Tableau validation

---

**Document generated:** 2026-04-07  
**Version:** EN (English)  
**Next versions:** RO (Română)

