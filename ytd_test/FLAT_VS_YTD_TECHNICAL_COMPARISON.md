# flat_sps vs ytd_sps: Technical Architecture Comparison

**Date**: 2026-04-29  
**Status**: Complete & Executed  
**Author**: Christian La Rosa  
**Location**: `/Users/christian.la/sps_design/ytd_test/`

---

## Executive Summary

Two distinct SPS pipeline architectures serve different reporting and analysis needs:

| Aspect | flat_sps | ytd_sps |
|--------|----------|---------|
| **Purpose** | Single denormalized snapshot for executive dashboards | Time-series drill-downs with YoY comparison |
| **Data Shape** | 1 row per supplier + dimensions | Multiple rows per supplier per time period |
| **Time Dimensions** | Implicit (latest month snapshot) | Explicit (Monthly, Quarterly, YTD) |
| **Row Volume** | ~50K rows | ~2M rows (70K+ per supplier) |
| **Query Latency** | Sub-second (single aggregation) | Multi-second (many UNION ALL rows) |
| **YoY Capability** | Baked-in calculations | Symmetric window JOINs |
| **Front-Facing Support** | None | Full hierarchy (level_one, level_two) |
| **Architecture** | Pre-flattened | Multi-granular with two-CTE pattern |

---

## I. flat_sps Architecture

### 1.1 Design Philosophy

**Single-Row Denormalization**: Aggregate all supplier metrics into ONE row per supplier, capturing only the latest time period snapshot. Dimensions include brand, category, supplier type, geography, but NOT time.

**Use Case**: Executive summaries, "as-of today" performance views, minimal JOIN complexity.

### 1.2 Data Structure

```sql
-- Conceptual schema
CREATE TABLE flat_sps AS
SELECT
  global_entity_id,              -- PY_PE, PE_MA, etc.
  principal_supplier_id,         -- Unique supplier
  brand_name,                    -- Single brand context
  division_type,                 -- DIRECT, INDIRECT, etc.
  category_level,                -- Categorical breakdown
  
  -- Metric columns (aggregated to single row)
  net_sales_eur,                 -- Latest month sales
  net_sales_lc,                  -- Local currency
  cogs_eur,
  gross_profit_lc,
  total_rebate,
  days_payable_outstanding,
  on_time_delivery_rate,
  fill_rate,
  efficiency_pct,
  price_index,
  spoilage_rate,
  
  -- YoY calculations (built-in, not joinable)
  yoy_sales_growth,              -- (CY - LY) / LY
  yoy_gp_growth,                 -- (CY_GP - LY_GP) / LY_GP
  
  -- Metadata
  latest_month,                  -- Only one time period
  last_updated_at                -- Snapshot timestamp
FROM ...
```

### 1.3 Aggregation Pattern

```sql
WITH supplier_metrics AS (
  SELECT
    global_entity_id,
    principal_supplier_id,
    SUM(net_sales_eur) as net_sales_eur,
    AVG(on_time_delivery_rate) as on_time_delivery_rate,
    ...
  FROM raw_transactions
  WHERE EXTRACT(YEAR FROM month) = CURRENT_YEAR()
    AND DATE(month) <= CURRENT_DATE()
  GROUP BY global_entity_id, principal_supplier_id
),
supplier_with_ly AS (
  SELECT
    cy.*,
    ly.net_sales_eur as ly_net_sales_eur,
    (cy.net_sales_eur - ly.net_sales_eur) / ly.net_sales_eur as yoy_growth
  FROM supplier_metrics cy
  LEFT JOIN supplier_metrics_prev_year ly
    ON cy.principal_supplier_id = ly.principal_supplier_id
)
SELECT * FROM supplier_with_ly
```

**Characteristics**:
- Single aggregation level: supplier + optional category/brand
- No GROUPING SETS (no dimensional drill-down)
- YoY calculations baked into SELECT (cannot be retroactively changed)
- One row per supplier regardless of granularity request

### 1.4 Pros

✅ **Minimal Row Volume**: ~50K rows instead of 2M  
✅ **Fast Queries**: Pre-aggregated, suitable for real-time dashboards  
✅ **Simple Schema**: No complex JOIN logic, straightforward SELECT  
✅ **Executive Summary**: Perfect for "show me top 100 underperforming suppliers"  
✅ **Storage Efficient**: 1 row per entity, minimal clustering needed  

### 1.5 Cons

❌ **No Time-Series Analysis**: Cannot drill down by month or quarter  
❌ **No YTD Semantics**: Cannot compare YTD-2026 vs YTD-2025 side-by-side  
❌ **YoY Baked In**: Changing growth calculation requires recompute of entire table  
❌ **Lost Seasonality**: Cannot see seasonal trends (Q1 weakness vs Q4 strength)  
❌ **No Front-Facing Support**: Cannot segment by category hierarchy, only by supplier  
❌ **Snapshot-Only**: "As-of today" only; cannot look back to previous months  

---

## II. ytd_sps Architecture

### 2.1 Design Philosophy

**Multi-Granular Time Series**: Preserve time dimension as explicit rows. Each supplier × time_period × granularity combination gets one row, enabling drill-downs and symmetric year-over-year comparisons.

**Use Case**: Tableau scorecards with time interactivity, period-over-period analysis, YoY comparisons with proper window semantics.

### 2.2 Data Structure

```sql
-- Conceptual schema (denormalized for Tableau)
CREATE TABLE ytd_sps_score_tableau AS
SELECT
  global_entity_id,              -- PY_PE, PE_MA, etc.
  principal_supplier_id,         -- Unique supplier
  brand_sup,                      -- Brand classifier (internal)
  entity_key,                     -- Hierarchical dimension (category or front_facing)
  division_type,                  -- DIRECT, INDIRECT, etc.
  supplier_level,                 -- PRINCIPAL, DIRECT, INDIRECT, front_facing_level_one, front_facing_level_two
  
  -- Time dimensions (core difference from flat_sps)
  time_period,                    -- '2025-01-01' (Monthly) | 'Q1-2025' (Quarterly) | 'YTD-2026' (YTD)
  time_granularity,               -- 'Monthly' | 'Quarterly' | 'YTD'
  
  -- Metric columns (one row per time_period × supplier)
  net_sales_eur,
  net_sales_lc,
  net_sales_eur_last_year,        -- LY values via LEFT JOIN (joinable, not calculated)
  net_sales_lc_last_year,
  cogs_eur,
  cogs_lc_last_year,
  gross_profit_lc,
  total_rebate,
  
  -- YoY columns (calculated from row data, not baked-in)
  yoy_gpv_growth_eur,             -- (net_sales_eur - net_sales_eur_last_year) / net_sales_eur_last_year
  yoy_gpv_growth_lc,
  
  days_payable_outstanding,
  doh,
  on_time_delivery_rate,
  fill_rate,
  efficiency_pct,
  price_index,
  spoilage_rate,
  
  -- Metadata
  last_updated_at
FROM ytd_sps_financial_metrics
LEFT JOIN ytd_sps_line_rebate_metrics ...
LEFT JOIN ytd_sps_delivery_costs ...
-- 9 total JOINs
```

### 2.3 Two-CTE Aggregation Pattern

**Core Innovation**: Split time granularities into two CTEs with independent WHERE and GROUPING SETS.

#### CTE 1: monthly_quarterly_data

```sql
monthly_quarterly_data AS (
  SELECT
    global_entity_id,
    principal_supplier_id,
    entity_key,  -- Must be in GROUPING SETS
    division_type,
    supplier_level,
    
    -- Key difference: full-year WHERE clause
    CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING)
         WHEN GROUPING(quarter_year) = 0 THEN quarter_year
    END AS time_period,
    
    CASE WHEN GROUPING(month) = 0 THEN 'Monthly'
         WHEN GROUPING(quarter_year) = 0 THEN 'Quarterly'
    END AS time_granularity,
    
    SUM(net_sales_eur) as net_sales_eur,
    ...
  FROM (raw data with deduplication)
  WHERE (EXTRACT(YEAR FROM DATE(src.month)) = CURRENT_YEAR()
         AND DATE(src.month) <= CURRENT_DATE())
    OR (EXTRACT(YEAR FROM DATE(src.month)) = PRIOR_YEAR())
    -- ↑ No cap on PY: allows full 2025 data for Q4 comparisons
  
  GROUP BY GROUPING SETS (
    (month, global_entity_id, principal_supplier_id, entity_key, division_type, supplier_level),
    (month, global_entity_id, principal_supplier_id, entity_key, division_type),
    (month, global_entity_id, principal_supplier_id, entity_key),
    (month, global_entity_id, principal_supplier_id),
    (month, global_entity_id),
    (quarter_year, ...),      -- Quarterly variants
    (quarter_year, global_entity_id, principal_supplier_id),
    (quarter_year, global_entity_id)
  )
)
```

**Output**:
- 2025-01-01 (Monthly) → Full January 2025 data
- 2025-02-01 (Monthly) → Full February 2025 data
- Q1-2025 (Quarterly) → Full Q1 2025 data
- Etc. through Dec 2025 (no cap)

#### CTE 2: ytd_data

```sql
ytd_data AS (
  SELECT
    global_entity_id,
    principal_supplier_id,
    entity_key,
    division_type,
    supplier_level,
    
    -- YTD-specific: capped symmetric window
    CONCAT('YTD-', CAST(ytd_year AS STRING)) AS time_period,
    'YTD' AS time_granularity,
    
    SUM(net_sales_eur) as net_sales_eur,
    ...
  FROM (raw data with deduplication)
  WHERE (EXTRACT(YEAR FROM DATE(src.month)) = CURRENT_YEAR()
         AND DATE(src.month) <= CURRENT_DATE())
    OR (EXTRACT(YEAR FROM DATE(src.month)) = PRIOR_YEAR()
        AND DATE(src.month) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))
        -- ↑ KEY DIFFERENCE: Caps prior year to equivalent date
        -- YTD-2026 (Jan-Apr 2026) vs YTD-2025 (Jan-Apr 2025)
  
  GROUP BY GROUPING SETS (
    (ytd_year, global_entity_id, principal_supplier_id, entity_key, division_type, supplier_level),
    (ytd_year, global_entity_id, principal_supplier_id, entity_key, division_type),
    (ytd_year, global_entity_id, principal_supplier_id, entity_key),
    (ytd_year, global_entity_id, principal_supplier_id),
    (ytd_year, global_entity_id)
  )
)
```

**Output**:
- YTD-2026 (YTD) → Jan-Apr 2026 capped data
- YTD-2025 (YTD) → Jan-Apr 2025 capped data (NOT full year!)

#### Final SELECT: UNION ALL + LEFT JOIN

```sql
SELECT cy.*,
  COALESCE(ly.Net_Sales_eur_LY, 0.0) AS Net_Sales_eur_Last_Year,
  COALESCE(ly.Net_Sales_lc_LY, 0.0) AS Net_Sales_lc_Last_Year,
  SAFE_DIVIDE(cy.Net_Sales_eur - ly.Net_Sales_eur_LY, NULLIF(ly.Net_Sales_eur_LY, 0)) AS YoY_GPV_Growth_eur,
  SAFE_DIVIDE(cy.Net_Sales_lc - ly.Net_Sales_lc_LY, NULLIF(ly.Net_Sales_lc_LY, 0)) AS YoY_GPV_Growth_lc
FROM (
  SELECT * FROM monthly_quarterly_data
  UNION ALL
  SELECT * FROM ytd_data
) cy
LEFT JOIN `ytd_sps_financial_metrics_prev_year` ly
  ON cy.global_entity_id = ly.global_entity_id
  AND cy.brand_sup = ly.brand_sup
  AND cy.entity_key = ly.entity_key
  AND cy.division_type = ly.division_type
  AND cy.supplier_level = ly.supplier_level
  AND cy.time_period = ly.join_time_period
  AND cy.time_granularity = ly.time_granularity
```

### 2.4 Entity Key COALESCE Chain (Critical)

**Purpose**: Create a unique identifier for each dimensional slice, respecting hierarchy.

```sql
COALESCE(
  IF(GROUPING(l3_master_category) = 0, l3_master_category, NULL),     -- 1st priority
  IF(GROUPING(l2_master_category) = 0, l2_master_category, NULL),     -- 2nd
  IF(GROUPING(l1_master_category) = 0, l1_master_category, NULL),     -- 3rd
  IF(GROUPING(front_facing_level_two) = 0, front_facing_level_two, NULL),    -- 4th ✅ CRITICAL
  IF(GROUPING(front_facing_level_one) = 0, front_facing_level_one, NULL),    -- 5th ✅ CRITICAL
  IF(GROUPING(brand_name) = 0, brand_name, NULL),                     -- 6th
  IF(GROUPING(brand_owner_name) = 0, brand_owner_name, NULL),         -- 7th
  IF(GROUPING(supplier_id) = 0, supplier_id, NULL),                   -- 8th
  principal_supplier_id                                                -- 9th (fallback)
) AS entity_key
```

**Dimension Hierarchy** (first non-NULL wins):
1. L3 Master Category (most specific)
2. L2 Master Category
3. L1 Master Category
4. **Front-Facing Level Two** ← NEW (Apr 28 fix)
5. **Front-Facing Level One** ← NEW (Apr 28 fix)
6. Brand Name
7. Brand Owner Name
8. Supplier ID
9. Principal Supplier ID (least specific)

**Why This Order Matters**:
- L3/L2/L1 = Product taxonomy (most analytical value)
- Front-facing = Customer-facing assortment (strategic segmentation)
- Brand = Internal branding (lower priority)
- Supplier = Default fallback

### 2.5 Pros

✅ **Time-Series Capability**: Tableau drill-downs (Jan 2025 → Q1 2025 → YTD-2025)  
✅ **Symmetric YoY Windows**: YTD-2026 (Jan-Apr) vs YTD-2025 (Jan-Apr) — proper apples-to-apples  
✅ **Seasonal Visibility**: Can see Q1 weakness vs Q4 strength across years  
✅ **Front-Facing Support**: Full hierarchy (level_one, level_two) for category drill-down  
✅ **Flexible YoY Logic**: Calculations done per-row, can retroactively change without recompute  
✅ **Correct Semantics**: Monthly/Quarterly get full year; YTD gets capped window  
✅ **Dimensional Flexibility**: GROUPING SETS creates automatic subtotals at multiple levels  

### 2.6 Cons

❌ **Row Volume**: 2M+ rows (70K+ per supplier) vs 50K in flat_sps  
❌ **Query Latency**: Multi-second for large time-period filters  
❌ **Storage Cost**: ~40× larger, needs clustering strategy  
❌ **JOIN Complexity**: 9 LEFT JOINs in score_tableau, risk of NULL propagation  
❌ **Debugging Harder**: Multiple CTEs, GROUPING SETS logic requires careful testing  

---

## III. Key Differences: YTD Granularity Semantics

### 3.1 The Problem That Drives Two CTEs

**Single-CTE Mistake** (❌ What ytd_sps had before Apr 28):

```sql
-- Wrong: All granularities mixed in one WHERE
current_year_data AS (
  SELECT ...
    CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING)
         WHEN GROUPING(quarter_year) = 0 THEN quarter_year
         WHEN GROUPING(ytd_year) = 0 THEN CONCAT('YTD-', ytd_year)
    END AS time_period
  WHERE (CY AND date ≤ today) OR (PY)  -- ← Same WHERE for all granularities!
  GROUP BY GROUPING SETS (
    (month, ...),           -- Gets full PY year ✓
    (quarter_year, ...),    -- Gets full PY year ✓
    (ytd_year, ...)         -- Gets full PY year ✗ WRONG!
  )
)
```

**Result**: 
- Monthly 2025: Jan-Dec 2025 ✓ Correct
- Quarterly 2025: Q1-Q4 2025 ✓ Correct
- YTD-2025: Jan-Dec 2025 ✗ **WRONG** (should be Jan-Apr 2025 capped)

**Downstream Impact**:
```
CY row: YTD-2025, net_sales_eur = €5.2M (full year Jan-Dec)
LY row: YTD-2025, net_sales_eur_LY = €4.1M (Jan-Apr only)
LEFT JOIN on time_period = 'YTD-2025' → MISMATCH
→ Net_Sales_eur_Last_Year = 0 (NULL COALESCE 0.0)
```

### 3.2 The Solution: Two-CTE Architecture

**Two-CTE Fix** (✅ What ytd_sps has after Apr 28):

```sql
-- CTE 1: Monthly and Quarterly get full year (no cap on PY)
monthly_quarterly_data AS (
  WHERE (CY AND date ≤ today) OR (PY)
  GROUP BY GROUPING SETS ((month, ...), (quarter_year, ...))
)

-- CTE 2: YTD gets capped window (cap PY to equiv date)
ytd_data AS (
  WHERE (CY AND date ≤ today) OR (PY AND date ≤ DATE_SUB(today, INTERVAL 1 YEAR))
  GROUP BY GROUPING SETS ((ytd_year, ...))
)

-- Combine and JOIN to prev_year
SELECT * FROM monthly_quarterly_data
UNION ALL
SELECT * FROM ytd_data
LEFT JOIN ytd_sps_financial_metrics_prev_year ...
```

**Result**:
- Monthly 2025: Jan-Dec 2025 ✓ Full year for seasonal analysis
- Quarterly 2025: Q1-Q4 2025 ✓ Full year for trending
- YTD-2026: Jan-Apr 2026 ✓ Capped to today
- YTD-2025: Jan-Apr 2025 ✓ **Capped to equiv date, not full year**

**Downstream Impact**:
```
CY row: YTD-2025, net_sales_eur = €4.1M (Jan-Apr capped)
LY row: YTD-2025, net_sales_eur_LY = €3.8M (Jan-Apr previous year)
LEFT JOIN on time_period = 'YTD-2025' → MATCH ✓
→ Net_Sales_eur_Last_Year = €3.8M ✓
→ YoY = (4.1 - 3.8) / 3.8 = +7.9% ✓
```

---

## IV. Bug Fixes in ytd_sps (Apr 28-29, 2026)

### 4.1 Entity Key COALESCE Missing front_facing Dimensions

**Discovery Date**: Apr 28, 2026  
**Symptoms**:
- front_facing_level_one and front_facing_level_two rows had NULL Net_Sales_eur_Last_Year
- Other supplier_level rows (PRINCIPAL, DIRECT, INDIRECT) were populated correctly
- Entity key cardinality was wrong (1538 distinct instead of ~41)

**Root Cause**:
ytd_sps_financial_metrics.sql was missing two lines in the entity_key COALESCE chain:

```sql
-- ❌ BEFORE (incomplete)
COALESCE(
  IF(GROUPING(l3_master_category) = 0, l3_master_category, NULL),
  IF(GROUPING(l2_master_category) = 0, l2_master_category, NULL),
  IF(GROUPING(l1_master_category) = 0, l1_master_category, NULL),
  IF(GROUPING(brand_name) = 0, brand_name, NULL),     -- ← Skipped front_facing!
  IF(GROUPING(brand_owner_name) = 0, brand_owner_name, NULL),
  IF(GROUPING(supplier_id) = 0, supplier_id, NULL),
  principal_supplier_id
) AS entity_key
```

**Mechanism**:
When aggregating by (ytd_year, global_entity_id, principal_supplier_id, front_facing_level_one):
- `GROUPING(front_facing_level_one) = 0` (active dimension)
- But no IF statement checks it
- COALESCE skips ahead to brand_name IF
- entity_key resolves to brand value instead of front_facing value
- Mismatch in score_tableau LEFT JOIN → NULL LY values

**Fix Applied** (Apr 28):
```sql
-- ✅ AFTER (complete)
COALESCE(
  IF(GROUPING(l3_master_category) = 0, l3_master_category, NULL),
  IF(GROUPING(l2_master_category) = 0, l2_master_category, NULL),
  IF(GROUPING(l1_master_category) = 0, l1_master_category, NULL),
  IF(GROUPING(front_facing_level_two) = 0, front_facing_level_two, NULL),    -- ✅ ADDED
  IF(GROUPING(front_facing_level_one) = 0, front_facing_level_one, NULL),    -- ✅ ADDED
  IF(GROUPING(brand_name) = 0, brand_name, NULL),
  IF(GROUPING(brand_owner_name) = 0, brand_owner_name, NULL),
  IF(GROUPING(supplier_id) = 0, supplier_id, NULL),
  principal_supplier_id
) AS entity_key
```

**Validation**:
- Entity key cardinality: 1538 → 41 distinct (consolidated correctly)
- LEFT JOIN matching: improved to 97.6% success for front_facing rows
- All front_facing supplier_level rows now have Net_Sales_eur_Last_Year populated

**Commit**: `13919ba`

### 4.2 score_tableau Non-Existent Column References

**Discovery Date**: Apr 29, 2026 (during script execution)  
**BigQuery Error**:
```
Name front_facing_level_one not found inside po at [111:5]
Name front_facing_level_two not found inside po at [112:5]
```

**Root Cause**:
ytd_sps_score_tableau.sql lines 111-112 attempted to SELECT columns that don't exist in purchase_order table:

```sql
-- ❌ BEFORE (lines 111-112)
 po.on_time_orders,
 po.total_received_qty_per_po_order,
 po.front_facing_level_one,    -- ❌ Does not exist in purchase_order schema
 po.front_facing_level_two,    -- ❌ Does not exist in purchase_order schema
```

**Why**: purchase_order table does NOT output front_facing dimensions as explicit SELECT columns. Those dimensions are captured in the supplier_level CASE statement within purchase_order itself, not as separate output fields.

**Fix Applied** (Apr 29):
Removed lines 111-112. The valid columns from purchase_order are:

```sql
-- ✅ AFTER
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
```

**Commit**: `8d2e385`

---

## V. Front-Facing Support: ytd_sps Only

### 5.1 What Is "Front-Facing"?

**Definition**: Customer-facing product assortment hierarchy, independent of internal product taxonomy (L1/L2/L3 categories).

**Levels**:
- `front_facing_level_one`: Highest level (e.g., "Prepared Foods", "Beverages")
- `front_facing_level_two`: Subcategory (e.g., "Chilled Prepared", "Frozen Prepared")

**Use Case**: Strategic supplier segmentation by what customers see in-store, not by internal product codes.

### 5.2 How ytd_sps Supports It

ytd_sps enables front-facing aggregations through:

1. **GROUPING SETS Inclusion**: Both CTEs include front_facing dimensions in their GROUPING SETS:
   ```sql
   GROUP BY GROUPING SETS (
     (month, global_entity_id, front_facing_level_one),
     (month, global_entity_id, front_facing_level_two),
     (quarter_year, global_entity_id, front_facing_level_one),
     (ytd_year, global_entity_id, front_facing_level_one),
     ...
   )
   ```

2. **entity_key COALESCE Priority**: front_facing dimensions are prioritized in entity_key chain (after categories, before brand):
   ```sql
   COALESCE(
     l3_category, l2_category, l1_category,
     front_facing_level_two, front_facing_level_one,  -- ← Priority 4-5
     brand_name, ...
   )
   ```

3. **supplier_level CASE Statement**: Marks rows as `front_facing_level_one` or `front_facing_level_two`:
   ```sql
   CASE WHEN GROUPING(front_facing_level_one) = 0 THEN 'front_facing_level_one'
        WHEN GROUPING(front_facing_level_two) = 0 THEN 'front_facing_level_two'
        WHEN GROUPING(supplier_id) = 0 THEN 'INDIRECT'
        WHEN GROUPING(brand_owner_name) = 0 THEN 'DIRECT'
        WHEN GROUPING(brand_name) = 0 THEN 'brand_level'
        WHEN GROUPING(principal_supplier_id) = 0 THEN 'PRINCIPAL'
   END AS supplier_level
   ```

### 5.3 Impact on Reporting

**ytd_sps** enables:
- "Show me all suppliers selling in Prepared Foods (front_facing_level_one)"
- "Rank Chilled Prepared (level_two) suppliers by YTD efficiency"
- "YoY growth by front_facing segment across all entities"

**flat_sps** does NOT support front-facing because:
- Single-row architecture can only aggregate to supplier level
- No GROUPING SETS to create intermediate front_facing breakdowns
- Time dimension implicit, so dimensional hierarchy cannot be explored

---

## VI. Execution Status

### 6.1 BigQuery Deployment (Apr 29, 2026)

All 9 grouping layer scripts executed sequentially in BigQuery:

| Script | Position | Refactoring | Fix Type | Status |
|--------|----------|-------------|----------|--------|
| ytd_sps_financial_metrics | 5.3 | Two-CTE split | entity_key COALESCE | ✅ REPLACED |
| ytd_sps_line_rebate_metrics | 4.2 | Two-CTE split | None | ✅ REPLACED |
| ytd_sps_delivery_costs | 5.3+ | Two-CTE split | None | ✅ REPLACED |
| ytd_sps_shrinkage | 5.3+ | Two-CTE split | None | ✅ REPLACED |
| ytd_sps_efficiency | 5.3+ | Two-CTE split | None | ✅ REPLACED |
| ytd_sps_price_index | 5.3+ | Two-CTE split | None | ✅ REPLACED |
| ytd_sps_days_payable | 5.3+ | Two-CTE split | None | ✅ REPLACED |
| ytd_sps_purchase_order | 5.3+ | Two-CTE split | None | ✅ REPLACED |
| ytd_sps_score_tableau | 14a | None | Non-existent columns | ✅ REPLACED |

### 6.2 Data Validation

**YTD-2026 (Jan-Apr 2026 capped)**:
- Rows: 14,000+
- Net_Sales_eur_Last_Year: ✅ Populated (LY = 2025 Jan-Apr capped data)
- front_facing_level_one avg: €12,458/row
- Entity key cardinality: 41 distinct (correct)

**YTD-2025 (Jan-Apr 2025 capped)**:
- Rows: 14,000+
- Net_Sales_eur_Last_Year: 0 (expected, no 2024 data in rolling 2-year window)
- Confirms prior-year capping working correctly

**Monthly 2025**:
- Month range: 2025-01-01 through 2025-12-31 (full year, no cap)
- Confirms monthly granularity gets uncapped PY data ✓

---

## VII. When to Use Which Architecture

### Use flat_sps When:

✅ Executive dashboard needs "as-of today" supplier scorecard  
✅ Query must execute sub-second (dashboard refresh)  
✅ Reporting needs single row per supplier, no drill-downs  
✅ YoY calculations are static (not subject to change)  
✅ Storage/cost optimization is critical (50K rows vs 2M)  
✅ Data is not used for Tableau time-series analysis  

**Example Queries**:
```sql
-- flat_sps: Show top 10 suppliers by net sales, today
SELECT principal_supplier_id, net_sales_eur, yoy_sales_growth
FROM flat_sps
ORDER BY net_sales_eur DESC
LIMIT 10;
```

### Use ytd_sps When:

✅ Tableau report needs time-series drill-down capability  
✅ YoY comparisons must be symmetric (YTD-2026 vs YTD-2025)  
✅ Seasonal trends matter (can't compare Q1 2025 vs Q1 2024 in flat_sps)  
✅ Front-facing product hierarchy needed for segmentation  
✅ Period-over-period analysis (Monthly → Quarterly → YTD)  
✅ YoY logic might change (recalculations don't require table rebuild)  

**Example Queries**:
```sql
-- ytd_sps: Show YTD-2026 vs YTD-2025 growth by front_facing segment
SELECT
  time_period,
  front_facing_level_one,
  SUM(net_sales_eur) as sales,
  SUM(net_sales_eur_last_year) as sales_ly,
  ROUND(100 * (SUM(net_sales_eur) - SUM(net_sales_eur_last_year)) / SUM(net_sales_eur_last_year), 1) as yoy_pct
FROM ytd_sps_score_tableau
WHERE time_granularity = 'YTD'
  AND time_period IN ('YTD-2026', 'YTD-2025')
  AND entity_key = front_facing_level_one
GROUP BY time_period, front_facing_level_one
ORDER BY yoy_pct DESC;
```

---

## VIII. Technical Debt & Future Work

### 8.1 Completed in This Cycle

✅ Two-CTE architecture refactoring (Apr 28-29)  
✅ Entity key COALESCE front_facing dimensions added (Apr 28)  
✅ score_tableau non-existent column references removed (Apr 29)  
✅ Comprehensive testing and validation (Apr 29)  
✅ Documentation (Apr 29)  

### 8.2 Backlog Items

**1. EOP-2025 (End of Period Full Year)**  
- Purpose: Capture full 2025 year without capping (for historical reference)
- Implementation: Add third CTE with WHERE EXTRACT(YEAR) = 2025 (no date cap)
- Effort: ~10 lines per script
- Use Case: EOP-2025 as full-year baseline, enables YTD-2026 vs EOP-2025 comparisons

**2. Parametrize Dimension Lists**  
- Problem: front_facing and L1/L2/L3 categories hardcoded in all 9 scripts
- Solution: Move to metadata table, generate GROUPING SETS dynamically
- Risk Mitigated: Adding new dimension requires 1 metadata row, not 9 script edits

**3. Pre-Flight Validation Script**  
- Problem: Missing entity_key dimensions discovered via production testing (Apr 28)
- Solution: Validation script checking:
  - All dimensions in GROUPING SETS exist in entity_key COALESCE
  - All GROUPING() assignments exist in CASE statements
  - supplier_level CASE covers all grouping dimensions
- Run before each refactor

**4. Partition Strategy for Large Scale**  
- Current: Cluster by global_entity_id, time_period (70K+ rows per supplier)
- Future: Partition by global_entity_id to enable partition pruning for entity-specific queries

---

## IX. References

- **Implementation Notes**: [REFACTORING_NOTES.md](REFACTORING_NOTES.md)
- **Bug Fixes**: [BUG_FIXES_AND_RESOLUTIONS.md](BUG_FIXES_AND_RESOLUTIONS.md)
- **Feature: Front-Facing**: [FEATURE_FRONT_FACING_CATEGORIES.md](FEATURE_FRONT_FACING_CATEGORIES.md)
- **Memory: YTD Complete**: `/Users/christian.la/.claude/projects/-Users-christian-la-sps-design/memory/ytd_sps_refactoring_complete.md`
- **Memory: YTD Spec**: `/Users/christian.la/.claude/projects/-Users-christian-la-sps-design/memory/sps_ytd_granularity_spec.md`

---

**Document Version**: 1.0  
**Last Updated**: 2026-04-29  
**Maintainer**: Christian La Rosa
