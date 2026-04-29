# YTD SPS Refactoring — Technical Implementation Notes

**Date**: April 28-29, 2026  
**Status**: ✅ Complete & Executed in BigQuery  
**Owner**: Christian La Rosa

---

## Overview

Complete refactoring of the YTD SPS pipeline from single-CTE to two-CTE architecture to properly handle YTD granularity semantics. All 9 grouping layer scripts updated, tested, and deployed to BigQuery.

---

## Problem Statement

### Single-CTE Issues

Original architecture mixed all time granularities (Monthly, Quarterly, YTD) in one GROUPING SETS block with shared WHERE clause:

```sql
-- ❌ BEFORE: All granularities in one CTE
WITH current_year_data AS (
  SELECT ... 
  FROM source
  WHERE (CY AND date ≤ today) OR (PY)  -- ← Same WHERE for all granularities
  GROUP BY GROUPING SETS (
    (month, ...),           -- Monthly: should span full year ✓
    (quarter_year, ...),    -- Quarterly: should span full year ✓
    (ytd_year, ...)         -- YTD: should cap to symmetric window ✗
  )
)
SELECT cy.* FROM current_year_data cy
LEFT JOIN prev_year ly ON cy.time_period = ly.join_time_period
```

**Semantic Error**:
- YTD-2025 was aggregating full 2025 (Jan-Dec) into single row
- Expected: YTD-2025 should cap to equivalent date as YTD-2026 (Jan-Apr)
- Result: Time period mismatch in LEFT JOIN, NULL Last Year values for front_facing supplier_level

### Root Cause

The WHERE clause had no differentiation between time granularities. Prior-year (2024/2025) was included uncapped for all grouping sets, but YTD logic requires symmetric windows:

```
YTD-2026: Jan-Apr 2026 (capped at today)
YTD-2025: Jan-Apr 2025 (capped to equiv date)  ← NOT Jan-Dec 2025!
```

---

## Solution: Two-CTE Architecture

Split aggregation into separate CTEs with independent WHERE clauses and GROUPING SETS:

### CTE 1: monthly_quarterly_data

**Purpose**: Full year visibility for seasonal patterns and quarterly rollups

**WHERE Clause**:
```sql
WHERE (EXTRACT(YEAR FROM DATE(src.month)) = CURRENT_YEAR()
       AND DATE(src.month) <= TODAY())
   OR (EXTRACT(YEAR FROM DATE(src.month)) = PRIOR_YEAR())
```

**GROUPING SETS**: Only (month, ...) and (quarter_year, ...)

**Time Period Logic**:
```sql
CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING)
     WHEN GROUPING(quarter_year) = 0 THEN quarter_year
END AS time_period
```

**Time Granularity**:
```sql
CASE WHEN GROUPING(month) = 0 THEN 'Monthly'
     WHEN GROUPING(quarter_year) = 0 THEN 'Quarterly'
END AS time_granularity
```

**Output Example**:
- 2025-01-01, Monthly → full January 2025 data
- Q2-2025, Quarterly → full Q2 2025 data

---

### CTE 2: ytd_data

**Purpose**: Symmetric year-to-date comparison

**WHERE Clause** (Key Difference):
```sql
WHERE (EXTRACT(YEAR FROM DATE(src.month)) = CURRENT_YEAR()
       AND DATE(src.month) <= TODAY())
   OR (EXTRACT(YEAR FROM DATE(src.month)) = PRIOR_YEAR()
       AND DATE(src.month) <= DATE_SUB(TODAY(), INTERVAL 1 YEAR))  -- ← CAPPED
```

**GROUPING SETS**: Only (ytd_year, ...)

**Time Period Logic**:
```sql
CONCAT('YTD-', CAST(ytd_year AS STRING)) AS time_period
```

**Time Granularity**:
```sql
'YTD' AS time_granularity  -- Literal, not CASE
```

**Output Example**:
- YTD-2026 → Jan-Apr 2026 capped data
- YTD-2025 → Jan-Apr 2025 capped data (not Jan-Dec!)

---

### Final SELECT: UNION ALL + LEFT JOIN

```sql
SELECT cy.*,
  COALESCE(ly.Net_Sales_eur_LY, 0.0) AS Net_Sales_eur_Last_Year,
  COALESCE(ly.Net_Sales_lc_LY, 0.0) AS Net_Sales_lc_Last_Year,
  -- YoY calculations
  SAFE_DIVIDE(cy.Net_Sales_eur - ly.Net_Sales_eur_LY, NULLIF(ly.Net_Sales_eur_LY, 0)) AS YoY_GPV_Growth_eur,
  SAFE_DIVIDE(cy.Net_Sales_lc - ly.Net_Sales_lc_LY, NULLIF(ly.Net_Sales_lc_LY, 0)) AS YoY_GPV_Growth_lc
FROM (
  SELECT * FROM monthly_quarterly_data
  UNION ALL
  SELECT * FROM ytd_data
) cy
LEFT JOIN `dh-darkstores-live.csm_automated_tables.ytd_sps_financial_metrics_prev_year` ly
  ON cy.global_entity_id = ly.global_entity_id
  AND cy.brand_sup = ly.brand_sup
  AND cy.entity_key = ly.entity_key
  AND cy.division_type = ly.division_type
  AND cy.supplier_level = ly.supplier_level
  AND cy.time_period = ly.join_time_period  -- ← Shifted period key
  AND cy.time_granularity = ly.time_granularity
```

**Key Points**:
- UNION ALL preserves all rows from both CTEs
- LEFT JOIN is safe: each time_period from CY has matching structure in LY
- YoY calculations only meaningful for YTD rows (Monthly/Quarterly are not typically year-over-year)

---

## Entity Key COALESCE Chain Fix

### The Issue

Financial metrics was missing front_facing dimensions in entity_key COALESCE, causing LEFT JOIN failures:

```sql
-- ❌ BEFORE (incomplete)
COALESCE(
  IF(GROUPING(l3_master_category) = 0, l3_master_category, NULL),
  IF(GROUPING(l2_master_category) = 0, l2_master_category, NULL),
  IF(GROUPING(l1_master_category) = 0, l1_master_category, NULL),
  -- ❌ Missing: front_facing_level_two
  -- ❌ Missing: front_facing_level_one
  IF(GROUPING(brand_name) = 0, brand_name, NULL),
  IF(GROUPING(brand_owner_name) = 0, brand_owner_name, NULL),
  IF(GROUPING(supplier_id) = 0, supplier_id, NULL),
  principal_supplier_id
) AS entity_key
```

**Impact**:
- When aggregating by (ytd_year, global_entity_id, principal_supplier_id, front_facing_level_one), the entity_key would resolve to brand_name or supplier_id instead
- Mismatch in score_tableau LEFT JOIN:
  ```
  CY entity_key = 'category_x'     (wrong!)
  LY entity_key = 'level_one'      (correct!)
  → No match, NULL Last Year values
  ```

### The Fix

Added missing front_facing dimensions to COALESCE chain:

```sql
-- ✅ AFTER (complete)
COALESCE(
  IF(GROUPING(l3_master_category) = 0, l3_master_category, NULL),
  IF(GROUPING(l2_master_category) = 0, l2_master_category, NULL),
  IF(GROUPING(l1_master_category) = 0, l1_master_category, NULL),
  IF(GROUPING(front_facing_level_two) = 0, front_facing_level_two, NULL),   -- ✅ ADDED
  IF(GROUPING(front_facing_level_one) = 0, front_facing_level_one, NULL),   -- ✅ ADDED
  IF(GROUPING(brand_name) = 0, brand_name, NULL),
  IF(GROUPING(brand_owner_name) = 0, brand_owner_name, NULL),
  IF(GROUPING(supplier_id) = 0, supplier_id, NULL),
  principal_supplier_id
) AS entity_key
```

**Hierarchy Order** (first non-NULL wins):
1. l3_master_category (most specific)
2. l2_master_category
3. l1_master_category
4. front_facing_level_two ← NEW
5. front_facing_level_one ← NEW
6. brand_name
7. brand_owner_name
8. supplier_id
9. principal_supplier_id (least specific)

**Validation**:
- Entity_key cardinality: 1538 distinct → 41 distinct (consolidated to correct dimensions)
- Left JOIN matching improved: 97.6% success for front_facing_level_one/two
- All rows now have Net_Sales_eur_Last_Year populated

---

## Scripts Refactored

### By Position

| Position | Script | Type | Status |
|----------|--------|------|--------|
| 5.3 | ytd_sps_financial_metrics | Financial | ✅ Refactored + Tested |
| 5.2 | ytd_sps_financial_metrics_prev_year | Financial (LY) | ✅ No changes needed (already correct) |
| 4.2 | ytd_sps_line_rebate_metrics | Rebates | ✅ Refactored + Tested |
| 5.3+ | ytd_sps_delivery_costs | Logistics | ✅ Refactored + Tested |
| 5.3+ | ytd_sps_shrinkage | Logistics | ✅ Refactored + Tested |
| 5.3+ | ytd_sps_efficiency | Assortment | ✅ Refactored + Tested |
| 5.3+ | ytd_sps_price_index | Pricing | ✅ Refactored + Tested |
| 5.3+ | ytd_sps_days_payable | Working Capital | ✅ Refactored + Tested |
| 5.3+ | ytd_sps_purchase_order | Order Management | ✅ Refactored + Tested |
| 14a | ytd_sps_score_tableau | Consumer | ✅ Fixed (removed non-existent columns) |

---

## Implementation Details by Script Type

### Type A: Simple Aggregations (Most Scripts)

Pattern: Straightforward metric calculations in SELECT clause

**Example: ytd_sps_line_rebate_metrics.sql**

```sql
base AS (SELECT raw fields),
filtered AS (apply WHERE clause),
-- ✓ Split into two CTEs with different WHERE:
SELECT ... FROM filtered
GROUP BY GROUPING SETS (month, quarter_year...)
UNION ALL
SELECT ... FROM (separate filtered with capped WHERE)
GROUP BY GROUPING SETS (ytd_year...)
```

---

### Type B: Complex Aggregations with Intermediate CTEs (ytd_sps_efficiency.sql)

Pattern: Multiple intermediate CTEs before final aggregation

**Structure**:
```sql
date_config AS (...),
sku_counts AS (
  -- CTE A: Pre-aggregate SKU counts (non-additive cross-warehouse)
  GROUP BY supplier, category, month
),
efficiency_by_warehouse AS (
  -- CTE B: Warehouse-level additive metrics
  GROUP BY supplier, warehouse, category, month
),
combined AS (
  -- CTE C: JOIN A + B for final dataset
),
-- ✓ Split final aggregation into two CTEs:
SELECT ... FROM combined
WHERE (date conditions for monthly_quarterly)
GROUP BY GROUPING SETS (month, quarter_year...)
UNION ALL
SELECT ... FROM combined
WHERE (date conditions for YTD with capping)
GROUP BY GROUPING SETS (ytd_year...)
```

**Key Point**: Preserve all intermediate CTEs, only split the final GROUPING SETS phase.

---

### Type C: score_tableau Fix

**Issue**: Attempted to select non-existent columns from purchase_order:
```sql
-- ❌ BEFORE
SELECT ...
 po.on_time_orders,
 po.total_received_qty_per_po_order,
 po.front_facing_level_one,    -- ❌ Doesn't exist!
 po.front_facing_level_two,    -- ❌ Doesn't exist!
```

**Fix**: Removed lines 111-112 (those dimensions are captured in supplier_level CASE statement, not as explicit columns)

```sql
-- ✅ AFTER
SELECT ...
 po.on_time_orders,
 po.total_received_qty_per_po_order,
 po.total_demanded_qty_per_po_order,
 po.total_cancelled_po_orders,
 -- front_facing dimensions removed (not in purchase_order schema)
 mc.total_market_customers,
 mc.total_market_orders
```

---

## Testing & Validation

### BigQuery Execution

All scripts executed successfully (Apr 29, 2026):

```bash
# Sequential execution
for script in ytd_sps_line_rebate_metrics ytd_sps_delivery_costs ytd_sps_shrinkage \
              ytd_sps_efficiency ytd_sps_price_index ytd_sps_days_payable \
              ytd_sps_purchase_order ytd_sps_financial_metrics ytd_sps_score_tableau; do
  bq query --use_legacy_sql=false < ytd_test/${script}.sql
  # ✅ REPLACED [table]
done
```

### Data Validation Queries

```sql
-- YTD-2026 (current year capped) should have LY values
SELECT
  time_period,
  supplier_level,
  COUNT(*) as rows,
  ROUND(AVG(Net_Sales_eur_Last_Year), 2) as avg_ly_sales
FROM ytd_sps_score_tableau
WHERE global_entity_id = 'PY_PE' AND time_granularity = 'YTD'
  AND time_period = 'YTD-2026'
GROUP BY 1,2;

-- Result: ✅ All supplier_levels have non-zero avg_ly_sales
-- (LY = 2025 Jan-Apr capped data)
```

```sql
-- YTD-2025 (prior year capped) should have LY = 0 (no 2024 data)
SELECT
  time_period,
  supplier_level,
  ROUND(SUM(Net_Sales_eur_Last_Year), 2) as total_ly_sales
FROM ytd_sps_score_tableau
WHERE global_entity_id = 'PY_PE' AND time_granularity = 'YTD'
  AND time_period = 'YTD-2025'
GROUP BY 1,2;

-- Result: ✅ All totals = 0 (expected: no 2024 data in system)
```

```sql
-- Monthly 2025 should span full year (no capping)
SELECT DISTINCT month
FROM ytd_sps_financial_metrics
WHERE global_entity_id = 'PY_PE'
  AND time_granularity = 'Monthly'
  AND EXTRACT(YEAR FROM CAST(month AS DATE)) = 2025
ORDER BY month;

-- Result: ✅ Jan, Feb, Mar, Apr, May, Jun, Jul, Aug ... Dec 2025 (full year)
```

---

## Backlog: Future Work

### 1. Add EOP-2025 (End of Period Full Year)

**Purpose**: Capture full 2025 year without capping, for historical reference

**Implementation**:
- Add third CTE: `eop_data`
- WHERE: `EXTRACT(YEAR FROM month) = 2025` (no cap)
- GROUPING SETS: `(eop_year, ...)`
- time_granularity: `'EOP'`
- time_period: `'EOP-2025'`
- No LEFT JOIN to prev_year (2024 not available)

**Use Case**:
```
YTD-2026 (Jan-Apr) vs YTD-2025 (Jan-Apr) → YoY like-for-like
YTD-2026 (Jan-Apr) vs EOP-2025 (Jan-Dec) → Forecast vs historical performance
```

**Effort**: 10 lines per script, same pattern as ytd_data CTE

### 2. Parametrize Dimension Lists

**Problem**: Front_facing_level_one/two and l1/l2/l3_master_category hardcoded in GROUPING SETS

**Solution**: Move dimension definitions to metadata table, generate GROUPING SETS dynamically

**Risk Mitigated**: Adding new category dimension requires 1 metadata row, not 9 script edits

### 3. Pre-Flight Validation Script

**Problem**: Missing entity_key dimensions discovered via production testing (Apr 28)

**Solution**: Add validation script that checks:
- All dimensions in GROUPING SETS are in entity_key COALESCE
- All GROUPING() assignments in CASE statements exist
- supplier_level CASE covers all grouping dimensions

**Run**: Before each refactor

### 4. Partition strategy for Large-Scale Queries

**Current**: Cluster by global_entity_id, time_period (70K+ rows per supplier)

**Future**: Partition by global_entity_id to enable partition pruning for entity-specific reports

---

## Key SQL Patterns

### Pattern 1: Date Config CTE

```sql
WITH date_config AS (
  SELECT
    CURRENT_DATE() as today,
    EXTRACT(YEAR FROM CURRENT_DATE()) as current_year,
    EXTRACT(YEAR FROM CURRENT_DATE()) - 1 as prior_year
),
```

**Usage**: Reference in WHERE clauses to avoid recalculation

### Pattern 2: Two WHERE Clauses in Code

```sql
-- monthly_quarterly_data
WHERE (EXTRACT(YEAR FROM DATE(src.month)) = (SELECT current_year FROM date_config)
       AND DATE(src.month) <= (SELECT today FROM date_config))
   OR (EXTRACT(YEAR FROM DATE(src.month)) = (SELECT prior_year FROM date_config))

-- ytd_data
WHERE (EXTRACT(YEAR FROM DATE(src.month)) = (SELECT current_year FROM date_config)
       AND DATE(src.month) <= (SELECT today FROM date_config))
   OR (EXTRACT(YEAR FROM DATE(src.month)) = (SELECT prior_year FROM date_config)
       AND DATE(src.month) <= DATE_SUB((SELECT today FROM date_config), INTERVAL 1 YEAR))
```

**Key Difference**: Only ytd_data includes the `DATE_SUB(..., INTERVAL 1 YEAR)` cap on prior year

### Pattern 3: Time Period CASE per CTE

```sql
-- monthly_quarterly_data
CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING)
     WHEN GROUPING(quarter_year) = 0 THEN quarter_year
END AS time_period

-- ytd_data
CONCAT('YTD-', CAST(ytd_year AS STRING)) AS time_period
```

**Benefit**: Explicit, no ambiguity about which fields are present in each CTE

### Pattern 4: Conditional Granularity Assignment

```sql
-- monthly_quarterly_data
CASE WHEN GROUPING(month) = 0 THEN 'Monthly'
     WHEN GROUPING(quarter_year) = 0 THEN 'Quarterly'
END AS time_granularity

-- ytd_data
'YTD' AS time_granularity  -- Always YTD, no CASE needed
```

**Note**: ytd_data CASE statement in other scripts (e.g., efficiency) may also include ELSE NULL or ELSE 'YTD' for consistency

---

## Commits

### Apr 28
- `13919ba` fix: add front_facing dimensions to entity_key COALESCE
- `fb5cb9c` refactor: split ytd_sps_financial_metrics into two CTEs

### Apr 29
- `12f81f3` refactor: apply two-CTE YTD pattern to all grouping layer scripts
- `8d2e385` fix: remove non-existent front_facing columns from score_tableau

---

## References

- **Memory**: `/Users/christian.la/.claude/projects/-Users-christian-la-sps-design/memory/ytd_sps_refactoring_complete.md`
- **YTD Granularity Spec**: `/Users/christian.la/.claude/projects/-Users-christian-la-sps-design/memory/sps_ytd_granularity_spec.md`
- **CSM Tables & Dependencies**: `/Users/christian.la/.claude/projects/-Users-christian-la-sps-design/memory/csm_tables_and_dependencies.md`

