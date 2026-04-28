# Feature: Front-Facing Categories (Level 1 & 2)

**Status**: Design Phase  
**Date**: 2026-04-28  
**Scope**: YTD SPS DAG (ytd_test folder)

---

## 1. Objective

Add two new dimensions to the SPS DAG that capture the **most representative product category** (at level 1 and level 2) for each SKU in each country, extracted from the Catalog system.

These will enable supplier performance analysis by the **actual category customers see** (front-facing), rather than only internal master categories.

---

## 2. Data Source & Extraction Logic

### Source Table
- `fulfillment-dwh-production.cl_dmart.qc_catalog_products` (nested structure: `vendor_products[]` → `categories[]` → `category_details[]`)

### Extraction Strategy: "Most Common Across Vendors"

For each `(global_entity_id, sku)` pair:

1. **UNNEST** all vendors' category hierarchies
2. **Filter** to level 1 (or level 2) + `is_primary_category = TRUE` only
3. **Count** how many **distinct vendors** assign each category
4. **Select** the category with the highest vendor count
5. **Result**: One `front_facing_level_one` and one `front_facing_level_two` per SKU per country

### Why This Approach?
- **Deterministic**: Same answer every run
- **Vendor-consensus**: If 15 vendors say "Frutas y Verduras" and 1 says "Bebidas", we pick Frutas
- **No NULLs**: Every SKU in qc_catalog_products has at least one vendor with at least one primary category
- **Single value per SKU**: No multi-vendor conflicts in downstream aggregation

### Example
```
sku_id=36AOC3, global_entity_id=PY_PE
- Vendor 301851: Level1 = "Frutas y Verduras"
- Vendor 273453: Level1 = "Frutas y Verduras"
- Vendor 290397: Level1 = "Frutas y Verduras"
- ... (18 vendors total, all say "Frutas y Verduras")

→ front_facing_level_one = "Frutas y Verduras" (18/18 vendors agree)
```

---

## 3. Implementation Plan: Table-by-Table

### Table 1: `ytd_sps_product.sql`

**Change**: Add two subqueries in CTE `sku_sup_warehouse_qc_catalog` SELECT clause (after `level_three`)

**Location**: Line ~403 (after `qcp.level_three,`)

**Fields Added**:
```sql
-- front_facing_level_one
(
  SELECT cd.category_name_local
  FROM (
    SELECT
      cd.category_name_local,
      COUNT(DISTINCT vp.platform_vendor_id) as vendor_count
    FROM qc_catalog_products AS orig_qcp
      , UNNEST(orig_qcp.vendor_products) AS vp
      , UNNEST(vp.categories) AS c
      , UNNEST(c.category_details) AS cd
    WHERE orig_qcp.sku = qcp.sku
      AND orig_qcp.global_entity_id = qcp.global_entity_id
      AND c.category_level = 1
      AND cd.is_primary_category = TRUE
    GROUP BY cd.category_name_local
  )
  ORDER BY vendor_count DESC
  LIMIT 1
) AS front_facing_level_one,

-- front_facing_level_two (identical, but category_level = 2)
(... same structure, c.category_level = 2 ...)
AS front_facing_level_two,
```

**Impact**:
- Output: 1 row per (global_entity_id, sku, warehouse_id, supplier_id)
- Each row now has `front_facing_level_one` and `front_facing_level_two` (both strings, both safe defaults to '_unknown_')

**Schema Change**: Adds 2 columns

---

### Table 2: `ytd_sps_customer_orders.sql`

**Change**: Propagate the fields from `sps_product` through the order-level fact table

**Location 1** (CTE `tmp_sp_product`): Lines 18-41
- Add to SELECT: `COALESCE(sp.front_facing_level_one, '_unknown_') AS front_facing_level_one,`
- Add to SELECT: `COALESCE(sp.front_facing_level_two, '_unknown_') AS front_facing_level_two,`
- Update GROUP BY: add 2 new positions (from 17 to 19 columns)

**Location 2** (Final SELECT, around line 235):
- Add to SELECT:
  ```sql
  COALESCE(sp_exact.front_facing_level_one, sp_fallback.front_facing_level_one, '_unknown_') AS front_facing_level_one,
  COALESCE(sp_exact.front_facing_level_two, sp_fallback.front_facing_level_two, '_unknown_') AS front_facing_level_two,
  ```

**Impact**:
- Every order row now has its SKU's front-facing categories
- 1:1 cardinality with orders (same front_facing for all orders of same SKU)

**Schema Change**: Adds 2 columns

---

### Table 3: `ytd_sps_purchase_order_month.sql`

**Change**: Pull front-facing fields into the monthly PO aggregation

**Location** (CTE `tmp_sp_product`): Lines 21-37
- Add to SELECT: `COALESCE(sp.front_facing_level_one, '_unknown_') AS front_facing_level_one,`
- Add to SELECT: `COALESCE(sp.front_facing_level_two, '_unknown_') AS front_facing_level_two,`
- Update GROUP BY: add 2 new positions (from 9 to 11 columns)

**Impact**:
- These fields are now available for GROUPING SETS aggregations

**Schema Change**: Adds 2 columns

---

### Table 4: `ytd_sps_financial_metrics_month.sql`

**Change**: Pull from customer order facts

**Location** (Final SELECT, around line 39):
- Add after `l3_master_category,`:
  ```sql
  COALESCE(os.front_facing_level_one, '_unknown_') AS front_facing_level_one,
  COALESCE(os.front_facing_level_two, '_unknown_') AS front_facing_level_two,
  ```

**Impact**:
- Monthly financial metrics now have front-facing category context

**Schema Change**: Adds 2 columns

---

### Table 5: `ytd_sps_purchase_order.sql` (CRITICAL - GROUPING SETS)

**Change 1** (Field propagation): Around line 107
- Add to SELECT:
  ```sql
  ANY_VALUE(front_facing_level_one) AS front_facing_level_one,
  ANY_VALUE(front_facing_level_two) AS front_facing_level_two,
  ```

**Change 2** (GROUPING SETS): Lines 86-180 (add new groupings)

**Add after existing category deep-dives** (after line 150 or so):

```sql
    -- 4. FRONT-FACING CATEGORY DEEP-DIVE (By Owner + Front-Facing Categories)
    (month, global_entity_id, principal_supplier_id, front_facing_level_one),
    (month, global_entity_id, principal_supplier_id, front_facing_level_two),

    (month, global_entity_id, supplier_id, front_facing_level_one),
    (month, global_entity_id, supplier_id, front_facing_level_two),

    (month, global_entity_id, brand_owner_name, front_facing_level_one),
    (month, global_entity_id, brand_owner_name, front_facing_level_two),

    (month, global_entity_id, brand_name, front_facing_level_one),
    (month, global_entity_id, brand_name, front_facing_level_two),

    -- (repeat same 8 patterns for quarter_year)
    (quarter_year, global_entity_id, principal_supplier_id, front_facing_level_one),
    (quarter_year, global_entity_id, principal_supplier_id, front_facing_level_two),

    (quarter_year, global_entity_id, supplier_id, front_facing_level_one),
    (quarter_year, global_entity_id, supplier_id, front_facing_level_two),

    (quarter_year, global_entity_id, brand_owner_name, front_facing_level_one),
    (quarter_year, global_entity_id, brand_owner_name, front_facing_level_two),

    (quarter_year, global_entity_id, brand_name, front_facing_level_one),
    (quarter_year, global_entity_id, brand_name, front_facing_level_two),
```

**Change 3** (supplier_level CASE): Update the CASE statement (lines 56-62) to add:

```sql
CASE
    WHEN GROUPING(l3_master_category) = 0 THEN 'level_three' 
    WHEN GROUPING(l2_master_category) = 0 THEN 'level_two' 
    WHEN GROUPING(l1_master_category) = 0 THEN 'level_one' 
    WHEN GROUPING(front_facing_level_two) = 0 THEN 'front_facing_level_two'  -- NEW
    WHEN GROUPING(front_facing_level_one) = 0 THEN 'front_facing_level_one'  -- NEW
    WHEN GROUPING(brand_name) = 0 THEN 'brand_name'
    ELSE 'supplier' 
END AS supplier_level,
```

**Impact**:
- New `supplier_level` values: `'front_facing_level_one'` and `'front_facing_level_two'`
- Adds ~16 new rows per (month, global_entity_id, supplier) combination
- Enables analysis like "Show me this supplier's performance in Frutas y Verduras category"

**Schema Change**: Adds 2 columns; supplier_level cardinality increases

---

### Table 6: `ytd_sps_financial_metrics.sql`

**Change 1** (Field propagation): Around line 87
- Add to SELECT:
  ```sql
  ANY_VALUE(front_facing_level_one) AS front_facing_level_one,
  ANY_VALUE(front_facing_level_two) AS front_facing_level_two,
  ```

**Change 2** (GROUPING SETS): Lines 119-200 (identical to sps_purchase_order)

**Change 3** (supplier_level CASE): Identical to sps_purchase_order

**Impact**:
- Financial metrics aggregatable by front-facing category
- Enables margin/sales/cost analysis by actual customer-facing category

**Schema Change**: Adds 2 columns; supplier_level cardinality increases

---

### Table 7: `ytd_sps_score_tableau.sql`

**Change 1** (Field propagation): Around line 107
- Add to SELECT:
  ```sql
  po.front_facing_level_one,
  po.front_facing_level_two,
  ```
- **Exclude from sfm.*** (line 61): Update EXCEPT clause:
  ```sql
  sfm.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup, front_facing_level_one, front_facing_level_two),
  ```

**Impact**:
- Tableau dashboard can now filter/pivot by front-facing categories
- Available alongside all other metrics (OTD, fill rate, margin, etc.)

**Schema Change**: Adds 2 columns

---

## 4. Data Flow Diagram

```
ytd_sps_product
    ↓ (adds front_facing_level_one, front_facing_level_two)
ytd_sps_customer_orders
    ↓
ytd_sps_purchase_order_month (+ ytd_sps_financial_metrics_month)
    ↓
ytd_sps_purchase_order (GROUPING SETS aggregation + supplier_level classification)
ytd_sps_financial_metrics (GROUPING SETS aggregation + supplier_level classification)
    ↓
ytd_sps_score_tableau
    ↓
Tableau Dashboard
```

---

## 5. Impact Summary

### New supplier_level Values
- `'front_facing_level_one'` - when grouped by front_facing_level_one only
- `'front_facing_level_two'` - when grouped by front_facing_level_two only

### Table Impact
| Table | Rows Added | Columns Added | Notes |
|-------|-----------|---------------|-------|
| ytd_sps_product | 0 (same cardinality) | 2 | One per (entity, sku, warehouse, supplier) |
| ytd_sps_customer_orders | 0 | 2 | Propagated from product |
| ytd_sps_purchase_order_month | 0 | 2 | Propagated from customer orders |
| ytd_sps_financial_metrics_month | 0 | 2 | Propagated from customer orders |
| ytd_sps_purchase_order | ~16x more rows* | 2 | GROUPING SETS multiplier from new dimension |
| ytd_sps_financial_metrics | ~16x more rows* | 2 | GROUPING SETS multiplier from new dimension |
| ytd_sps_score_tableau | ~16x more rows | 2 | Cartesian from all_keys union |

*Approximate: actual increase depends on distinct values of (principal_supplier_id, supplier_id, brand_owner, brand_name, front_facing_level_one, front_facing_level_two)

---

## 6. Validation Queries

### Query 1: Uniqueness Check
```sql
-- Should return 0 rows (no SKU should have multiple front_facing values)
WITH check_distinctness AS (
  SELECT
    global_entity_id,
    sku,
    COUNT(DISTINCT front_facing_level_one) AS distinct_level_one,
    COUNT(DISTINCT front_facing_level_two) AS distinct_level_two
  FROM ytd_sps_product
  GROUP BY global_entity_id, sku
)
SELECT *
FROM check_distinctness
WHERE distinct_level_one > 1 OR distinct_level_two > 1;
```

### Query 2: Null Check
```sql
-- Should return 0 rows (no '_unknown_' should appear in production)
SELECT COUNT(*) as null_count
FROM ytd_sps_score_tableau
WHERE front_facing_level_one = '_unknown_' OR front_facing_level_two = '_unknown_';
```

### Query 3: supplier_level Distribution
```sql
SELECT supplier_level, COUNT(*) as row_count
FROM ytd_sps_purchase_order
WHERE time_granularity = 'Monthly'
GROUP BY supplier_level
ORDER BY row_count DESC;
```

---

## 7. Open Questions / Risks

1. **Performance**: Subqueries in SELECT clause of sps_product might be slow. Consider window functions or temp table instead.
2. **Nulls**: What if a SKU has no vendor with a primary category at level 2? Current code returns NULL, then COALESCE to '_unknown_'. Is this acceptable?
3. **Cardinality explosion**: GROUPING SETS adds ~16 new rows per supplier per time period. Is ytd_sps_purchase_order already optimized for high cardinality?
4. **GROUPING() logic**: supplier_level CASE must check front_facing fields BEFORE brand_name/supplier_id to prioritize front-facing when present. Order matters.

---

## 8. Rollback Plan

If issues arise:
1. Remove fields from ytd_sps_product CTE
2. Remove fields from downstream tables (comment out COALESCE lines)
3. Remove GROUPING SETS entries and supplier_level CASE updates
4. Revert ytd_sps_score_tableau SELECT and EXCEPT clauses

---

## Next Steps

1. **Review** this document for correctness
2. **Validate** extraction logic (uniqueness query)
3. **Implement** in order: sps_product → customer_orders → purchase_order_month → financial_metrics_month → purchase_order + financial_metrics → score_tableau
4. **Test** each table before moving to next
5. **Monitor** row counts and GROUPING SET cardinality
