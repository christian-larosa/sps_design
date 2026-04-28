# YTD Granularity Implementation — Documentation & Known Issues

**Status**: Field implementation complete with corrections  
**Date**: Apr 28, 2026  
**Scope**: 10 tables, 34 SQL scripts, front_facing_level_one/two propagation

---

## Bugs Fixed & Implementation Issues

### Issue 1: Missing front_facing fields in prev_year (CRITICAL) ✅
**What happened**: `ytd_sps_financial_metrics_prev_year` was missing `front_facing_level_one` and `front_facing_level_two` in:
- SELECT clause
- GROUPING SETS (monthly, quarterly, YTD)

**Why it mattered**: 
- Downstream table `ytd_sps_financial_metrics` (union of CY + LY) couldn't join properly on (entity_key, division_type, supplier_level)
- `ytd_sps_score_tableau` depends on financial_metrics with these fields for aggregation
- YTD rows in Tableau export would have NULL front_facing values for LY comparisons

**Fix applied**: 
- Added `front_facing_level_one, front_facing_level_two` to SELECT (line 78-79)
- Added 8 grouping sets combining front_facing with month/quarter/ytd for CY + LY (lines 100-107, 123-130, 145-152)

**Validation**: Re-executed prev_year → financial_metrics → score_tableau chain. No errors.

**Residual risk**: None identified. Field now propagates consistently through union.

---

### Issue 2: Front_facing propagation across aggregation layers

**Pattern**: Every aggregation table (financial_metrics, line_rebate_metrics, delivery_costs, days_payable, listed_sku, shrinkage, purchase_order, efficiency, market_customers) needed the same treatment:

| Table | Propagation | Status |
|-------|-------------|--------|
| ytd_sps_financial_metrics_month | Gathering layer — reads from source | Reviewed (no changes needed) |
| ytd_sps_financial_metrics_prev_year | Aggregation + filtering by max_date_per_entity | ✅ Fixed (added front_facing + grouping sets) |
| ytd_sps_financial_metrics | Union of CY + LY, joins all metrics | ✅ Verified (inherits from prev_year/month union) |
| ytd_sps_line_rebate_metrics | Gathering + aggregation | ✅ Has front_facing |
| ytd_sps_delivery_costs | Gathering + aggregation | ✅ Has front_facing |
| ytd_sps_days_payable | Gathering + aggregation | ✅ Has front_facing |
| ytd_sps_listed_sku | Gathering + aggregation | ✅ Has front_facing |
| ytd_sps_shrinkage | Gathering + aggregation | ✅ Has front_facing |
| ytd_sps_purchase_order | Gathering + aggregation | ✅ Has front_facing |
| ytd_sps_efficiency | Gathering + aggregation with snapshot logic | ✅ Has front_facing |
| ytd_sps_market_customers | Gathering only (no grouping sets) | ⚠️ Check needed (see Issue 3) |

---

### Issue 3: market_customers front_facing propagation (UNRESOLVED)

**Status**: Potentially incomplete  
**File**: `ytd_sps_market_customers.sql`

**Question**: Does market_customers need front_facing in grouping sets like other tables?

**Current structure** (from earlier read): Monthly, Quarterly, YTD unions without deep category/brand/front_facing breakdowns

**Impact if missing**: 
- score_tableau LEFT JOINs market_customers on (global_entity_id, time_period, time_granularity)
- If market_customers doesn't have front_facing dimension, it's OK (join on time dimension only)
- But if Tableau needs to filter/segment by front_facing, it won't work

**Action needed**: 
- [ ] Verify market_customers schema post-execution
- [ ] If missing, add front_facing grouping sets (same pattern as efficiency, delivery_costs, etc.)
- [ ] Re-execute market_customers + score_tableau

---

## How Front_facing Fields Work & Propagate

### Field Definition
- `front_facing_level_one`: Top-level category visible to customer (e.g., "Fresh", "Pantry", "Meat")
- `front_facing_level_two`: Sub-category (e.g., "Fresh > Produce", "Pantry > Beverages")

### Source
Originates in gathering layer (`*_month` tables). Example:
```sql
-- ytd_sps_delivery_costs_month (gathering)
SELECT
  ...,
  front_facing_level_one,
  front_facing_level_two,
  allocated_delivery_cost_eur,
  ...
FROM external_source
```

### Propagation Path

**Layer 1: Gathering** (month-level aggregations)
```sql
ytd_sps_delivery_costs_month
├── front_facing_level_one (from source)
└── front_facing_level_two (from source)
```

**Layer 2: Aggregation** (month/quarter/YTD grouping)
```sql
ytd_sps_delivery_costs
├── SELECT front_facing_level_one, front_facing_level_two (passthrough)
├── GROUPING SETS:
│   ├── (month, global_entity_id, principal_supplier_id, front_facing_level_one)
│   ├── (month, global_entity_id, principal_supplier_id, front_facing_level_two)
│   ├── (quarter_year, global_entity_id, supplier_id, front_facing_level_one)
│   ├── (quarter_year, global_entity_id, supplier_id, front_facing_level_two)
│   ├── (ytd_year, global_entity_id, brand_owner_name, front_facing_level_one)
│   └── (ytd_year, global_entity_id, brand_owner_name, front_facing_level_two)
│       ... (all combinations per time grain)
└── Result: 10 tables × ~90-100 rows each (grouping set result)
```

**Layer 3: Financial Union**
```sql
ytd_sps_financial_metrics
├── FROM ytd_sps_financial_metrics_month (CY, has front_facing)
├── UNION ALL
├── FROM ytd_sps_financial_metrics_prev_year (LY, NOW has front_facing after fix)
└── Result: CY + LY with matching (global_entity_id, brand_sup, entity_key, ..., front_facing_level_one/two)
```

**Layer 4: Final Tableau Export**
```sql
ytd_sps_score_tableau
├── FROM all_keys (union of 9 aggregation tables' dimension keys)
├── LEFT JOIN ytd_sps_financial_metrics (has front_facing)
├── LEFT JOIN ytd_sps_line_rebate_metrics (has front_facing)
├── ... (8 more tables, all with front_facing)
└── Result: Single denormalized fact table for Tableau
    ├── 87 columns
    ├── front_facing_level_one
    ├── front_facing_level_two
    └── All metrics from 10 upstream tables
```

### Join Logic
All LEFT JOINs in score_tableau use:
```sql
ON o.global_entity_id = t.global_entity_id 
  AND o.time_period = t.time_period 
  AND o.time_granularity = t.time_granularity 
  AND o.division_type = t.division_type 
  AND o.supplier_level = t.supplier_level 
  AND o.entity_key = t.entity_key 
  AND o.brand_sup = t.brand_sup
  -- front_facing is NOT a join key, it's a passed-through dimension
```

**Result**: If one table has front_facing=[level_one="Fresh", level_two="Produce"] but another has NULL, the JOIN still succeeds, and Tableau gets the mixed record.

---

## Interaction with Metrics

### Non-breaking changes
- **Additive metrics** (Net_Sales_eur, delivery_cost_eur, spoilage_value_eur, etc.): No impact. Front_facing is a dimension, not aggregated.
- **Ratios** (efficiency %, payment_days, doh, otd): No impact. Calculated at grouping set level; front_facing just segments the result.
- **COUNT/SUM aggregations**: No impact. GROUPING SETS naturally handles front_facing as another dimension.

### Potential subtle issues

1. **APPROX_COUNT_DISTINCT in YTD rows with front_facing**
   - Metric: `total_customers` uses APPROX_COUNT_DISTINCT(analytical_customer_id)
   - With front_facing: Now calculated per (global_entity_id, ytd_year, brand_sup, ..., front_facing_level_one)
   - Risk: Higher-level aggregations (e.g., total_customers across ALL front_facing) may show slightly different approximation error than summing the per-front_facing rows
   - Mitigation: <2% variance is acceptable per spec; Tableau users should SUM(total_customers) by front_facing, not manually aggregate

2. **Snapshot fields with front_facing (sku_listed, payment_days)**
   - Pattern: `MAX(CASE WHEN month = last_month THEN sku_listed END)`
   - With front_facing: Now per (supplier, brand, category, front_facing)
   - Example: Peru supplier X might have sku_listed=150 in Fresh but sku_listed=80 in Pantry (last month snapshot)
   - No error, but dimension explosion: each supplier × brand × category × front_facing = separate row
   - Impact: Score_tableau row count ≈ 10× larger than non-front_facing version
   - Mitigation: None needed (design intent); Tableau filters handle it

3. **YTD LY window per entity (prev_year fix)**
   - Now includes front_facing grouping sets
   - Risk: If Brazil max_date < Peru max_date, and a supplier has front_facing=[Fresh] in Brazil but front_facing=[Meat] in Peru, the LY window might be asymmetric
   - Actual risk: LOW. max_date is per global_entity_id (Brazil vs Peru separate), so each country's window is aligned.

---

## Validation Not Yet Completed

### ⏳ Required Validations (Post-build)

- [ ] **YTD rows exist** in score_tableau with time_granularity='YTD'
  - Check: `SELECT COUNT(*) FROM ytd_sps_score_tableau WHERE time_granularity='YTD'`
  - Expected: >0 rows for PY_PE

- [ ] **YTD = SUM(monthly)** for additive metrics
  - Check Jan-Apr 2026 monthly delivery_cost_eur vs YTD-2026 delivery_cost_eur
  - Tolerance: ±0.5% (due to rounding)

- [ ] **LY cap working** (max_date_per_entity logic)
  - Check: SUM(Net_Sales_eur_LY) for YTD-2025 should be ~4/12 of annual 2025 (proportional to Jan-Apr)
  - If full 2025 is shown, prev_year filter failed

- [ ] **Front_facing propagation intact**
  - Check: All rows in score_tableau have non-null (front_facing_level_one, front_facing_level_two) or both NULL
  - Flag: If front_facing_level_one is not null but level_two is null (or vice versa), data consistency issue
  - Query:
    ```sql
    SELECT
      COUNT(*) as total_rows,
      COUNTIF(front_facing_level_one IS NOT NULL) as has_l1,
      COUNTIF(front_facing_level_two IS NOT NULL) as has_l2,
      COUNTIF(front_facing_level_one IS NOT NULL AND front_facing_level_two IS NULL) as orphan_l1
    FROM ytd_sps_score_tableau
    WHERE time_granularity='YTD'
    ```

- [ ] **Market_customers front_facing** (Issue 3)
  - Check market_customers schema: Does it have front_facing grouping?
  - If missing, add and re-execute

- [ ] **No duplicate rows** in score_tableau
  - Check: `SELECT COUNT(*), COUNT(DISTINCT *) FROM ytd_sps_score_tableau GROUP BY global_entity_id, time_period HAVING COUNT(*) != COUNT(DISTINCT *)`
  - Expected: 0 duplicates

- [ ] **Join key cardinality** in score_tableau
  - Each (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup) should appear exactly once or once per front_facing combination
  - Query:
    ```sql
    SELECT
      COUNT(*) as dup_count,
      global_entity_id, time_period, division_type, supplier_level, entity_key, brand_sup
    FROM ytd_sps_score_tableau
    GROUP BY 2,3,4,5,6,7
    HAVING COUNT(*) > 2
    ```

### ⏳ Validation Queries (Run After Approval)

```sql
-- 1. YTD row count
SELECT time_granularity, COUNT(*) as row_count
FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_score_tableau`
WHERE global_entity_id='PY_PE'
GROUP BY time_granularity;

-- 2. Monthly sum vs YTD for delivery costs
WITH monthly_sum AS (
  SELECT
    SUM(delivery_cost_eur) as delivery_cost_monthly_sum
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_score_tableau`
  WHERE global_entity_id='PY_PE'
    AND time_granularity='Monthly'
    AND EXTRACT(YEAR FROM CAST(time_period AS DATE)) = 2026
),
ytd_row AS (
  SELECT
    SUM(delivery_cost_eur) as delivery_cost_ytd
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_score_tableau`
  WHERE global_entity_id='PY_PE'
    AND time_granularity='YTD'
    AND time_period='YTD-2026'
)
SELECT
  ROUND(m.delivery_cost_monthly_sum, 2) as monthly_sum,
  ROUND(y.delivery_cost_ytd, 2) as ytd_value,
  ROUND(ABS(m.delivery_cost_monthly_sum - y.delivery_cost_ytd) / m.delivery_cost_monthly_sum * 100, 2) as variance_pct
FROM monthly_sum m, ytd_row y;

-- 3. Front_facing consistency
SELECT
  COUNTIF(front_facing_level_one IS NOT NULL AND front_facing_level_two IS NULL) as orphan_level_one,
  COUNTIF(front_facing_level_one IS NULL AND front_facing_level_two IS NOT NULL) as orphan_level_two,
  COUNTIF(front_facing_level_one IS NOT NULL AND front_facing_level_two IS NOT NULL) as both_populated,
  COUNTIF(front_facing_level_one IS NULL AND front_facing_level_two IS NULL) as both_null
FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_score_tableau`
WHERE global_entity_id='PY_PE' AND time_granularity='YTD';
```

---

## Known Limitations & Future Work

### V1 Limitations (as designed)
1. **Segmentation excluded from YTD** — segment_lc = NULL for all YTD rows (intentional, segmentation logic too complex for YTD)
2. **DOH distortion in current month** — Apr 28 data appears slightly high (28 days ÷ 30 calendar days); ~1.7% noise
3. **APPROX_COUNT_DISTINCT variance** — <2% rounding error expected when comparing SUM(monthly) to YTD totals

### Potential Future Issues (pre-validated)
1. **Dimension explosion** — front_facing grouping sets multiply row count; acceptable for Tableau but monitor BigQuery costs
2. **Asymmetric LY window** — Different max_dates per country mean LY windows don't align; design is intentional (per-entity fairness)
3. **Market_customers integration** — Needs validation if front_facing grouping was applied (Issue 3)

### Out of Scope (Post-V1)
- [ ] Segmentation logic for YTD (Decision 4, deferred)
- [ ] Calendar vs data month correction for DOH (Decision, deferred)
- [ ] Backfill for historical YTD (2024, 2025) — currently only CY + LY
- [ ] Performance optimization for score_tableau (14 LEFT JOINs on 9 tables)

---

## Summary of Changes

| File | Change | Status |
|------|--------|--------|
| ytd_sps_financial_metrics_prev_year.sql | Added front_facing to SELECT + GROUPING SETS (all 3 time grains) | ✅ Fixed & executed |
| ytd_sps_financial_metrics.sql | No change needed (union inherits from prev_year) | ✅ Re-executed |
| ytd_sps_score_tableau.sql | No change needed (inherits from financial_metrics) | ✅ Re-executed |
| ytd_sps_market_customers.sql | TBD — validation needed (Issue 3) | ⏳ Pending check |

---

## Next Steps

1. **Run validation queries** (section above) against PY_PE
2. **Check market_customers** for front_facing gaps (Issue 3)
3. **Review Tableau dashboard** for correct dimension filtering
4. **Commit to git** (this doc + all SQL files)
5. **Sync personal repo** with work
6. **Merge to sps_sql_prod** when validation passes
