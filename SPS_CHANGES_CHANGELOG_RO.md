# 📚 MANUAL DE MODIFICĂRI SPS | Versiune România (RO)

**Document de referință pentru modificări: sps_originals → flat_sps_***  
**Audientă:** Analiști de date senior  
**Format:** Pe tabel, în ordinea DAG  
**Ultima actualizare:** 2026-04-07

---

## 🗂️ ORDINEA DAG

```
1. sps_product (BAZĂ)
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

   └─ sps_score_tableau (FINAL - depinde de TOATE)
```

---

## 📊 TABEL 1: sps_product (BAZĂ)

**Poziție în DAG:** Rădăcină  
**Dependențe:** Niciunul (tabel de bază)

### 📈 Modificări detectate

| Aspect | Înainte | După | Impact |
|--------|---------|-------|--------|
| Linii | 808 | 806 | -2 linii (curățare) |
| CTE-uri | 1 (simplu) | 1 (simplu) | Fără modificări |
| Clauze FROM | Tabele interne multiple | Tabele interne multiple | Fără modificări semnificative |

### ✅ Verificare

```bash
# Diferența totală
diff sps_product.sql flat_sps_product.sql | wc -l
# Output: 113 modificări (mai mult hardcodes de parametri/dataset)
```

### 📝 Concluzie

**Nu sunt modificări substanțiale în logică sau câmpuri.** Doar:
- Hardcodes de proiect/dataset (`{{ params.project_id }}` → `dh-darkstores-live`)
- Posibile modificări minore de format/spații

**Recomandare:** Procedează ca bază de încredere. Validează că aceleași SKU-uri există.

---

## 📊 TABEL 2: sps_efficiency_month

**Poziție în DAG:** Nivel 2 (depinde de: sps_product, date AQS)  
**Dependențe:** sps_product, sku_efficiency_detail_v2  
**Criticitate:** 🔴 **FOARTE RIDICATĂ** - Schimbare de metodologie AQS

### 🔴 MODIFICARE CRITICĂ: AQS v5 → AQS v2 (implementează AQS v7)

#### Schimbare sursă date

| Parametru | sps_originals (AQS v5) | flat_sps (AQS v2) | Implicație |
|-----------|------------------------|-------------------|------------|
| **Tabel** | `_aqs_v5_sku_efficiency_detail` | `sku_efficiency_detail_v2` | Noua metodologie |
| **Dataset** | `{{ params.dataset.rl }}` | `fulfillment-dwh-production.rl_dmart` | Schimbare locație |
| **Partiție** | `partition_month` | `partition_month` | Identic (compatibil) |

#### Modificări câmpuri - COMPARAȚIE DIRECTĂ

| Câmp | Înainte (v5) | După (v2/v7) | Tip | Impact |
|-------|-----------|-----------------|------|--------|
| `sku_efficiency` | ❌ NU EXISTĂ | ✅ NOU (ENUM) | NOU | **CRITIC** - Categorizare predefinită |
| `updated_sku_age` | ❌ NU EXISTĂ | ✅ NOU (INT) | NOU | Înlocuiește `date_diff` |
| `available_hours` | ❌ NU EXISTĂ | ✅ NOU | NOU | Nouă metrică disponibilitate |
| `potential_hours` | ❌ NU EXISTĂ | ✅ NOU | NOU | Nouă metrică disponibilitate |
| `numerator_new_avail` | ❌ NU EXISTĂ | ✅ NOU | NOU | Ingredient pentru disponibilitate ponderată |
| `denom_new_avail` | ❌ NU EXISTĂ | ✅ NOU | NOU | Ingredient pentru disponibilitate ponderată |
| `sku_status` | ❌ NU EXISTĂ | ✅ NOU | NOU | Status SKU |
| `is_listed` | ❌ NU EXISTĂ | ✅ NOU (BOOL) | NOU | Indicator listare |
| `date_diff` | ✅ EXISTĂ | ❌ ÎNLĂTURAT | ÎNLĂTURAT | Înlocuit prin `updated_sku_age` |
| `avg_qty_sold` | ✅ EXISTĂ | ❌ ÎNLĂTURAT | ÎNLĂTURAT | Acum vine ca categorie în `sku_efficiency` |
| `new_availability` | ✅ EXISTĂ | ❌ ÎNLĂTURAT (dar trăiește în ingrediente) | ÎNLĂTURAT | Descompus în numerator/denom |

### 📐 Schimbare logică - AQS v5 vs AQS v7

**AQS v5 (înainte):**
```sql
-- Categorizare manuală bazată pe logică
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

**AQS v2/v7 (după):**
```sql
-- Câmp precalculat din sursă (sku_efficiency_detail_v2)
e.sku_efficiency  -- ENUM: 'efficient_sku', 'zero_mover', 'slow_mover', ...
```

### ⚠️ Implicații pentru downstream (sps_efficiency)

```sql
-- ÎNAINTE (v5):
COUNT(DISTINCT CASE WHEN date_diff >=90 AND (avg_qty_sold >= 1) THEN sku_id END) AS efficient_movers

-- DUPĂ (v2/v7):
COUNT(DISTINCT CASE WHEN updated_sku_age >= 90 AND sku_efficiency = 'efficient_sku' THEN sku_id END) AS efficient_movers
```

**Diferență conceptuală:**
- `date_diff` = zile de la prima listare (simplu)
- `updated_sku_age` = zile cu "reset logic" (dacă sunt schimbări warehouse, recalculează) → **Mai precis**

### 🔍 Validare recomandată

```sql
-- Verifică că ambele metodologii produc numărări similare
SELECT
  COUNT(DISTINCT CASE WHEN updated_sku_age >= 90 AND sku_efficiency = 'efficient_sku' THEN sku_id END) as v7_efficient,
  COUNT(DISTINCT CASE WHEN date_diff >= 90 AND avg_qty_sold >= 1 THEN sku_id END) as v5_efficient,
  (COUNT(*) OVER () - COUNT(DISTINCT CASE WHEN sku_efficiency IN ('efficient_sku', 'zero_mover', 'slow_mover') THEN sku_id END)) as unmapped_skus
FROM flat_sps_efficiency_month
WHERE CAST(month AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY global_entity_id, supplier_id;
```

### ✅ Concluzie

**SCHIMBARE METODOLOGICĂ IMPORTANTĂ dar COMPATIBILĂ.**  
- AQS v7 este mai precis (`updated_sku_age` cu reset logic)
- Categoriile (`sku_efficiency`) vin precalculate (mai puțin eroare manuală)
- Downstream (sps_efficiency) **depinde** de aceste schimbări → **validează totaluri**

---

## 📊 TABEL 3: sps_efficiency

**Poziție în DAG:** Nivel 3 (depinde de: sps_efficiency_month)  
**Dependențe:** sps_efficiency_month  
**Criticitate:** 🔴 **FOARTE RIDICATĂ** - Refactoring arhitectural + nouă formulă

### 🏗️ SCHIMBARE ARHITECTURALĂ: 1 CTE → 3 CTE-uri

#### Înainte (sps_originals)

```sql
-- Structură simplă: 1 SELECT direct + GROUPING SETS
SELECT
  global_entity_id,
  CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING) ELSE quarter_year END AS time_period,
  -- ... dimensiuni ...
  COUNT(DISTINCT sku_id) AS sku_listed,
  COUNT(DISTINCT CASE WHEN date_diff >= 90 THEN sku_id END) AS sku_mature,
  COUNT(DISTINCT CASE WHEN ... AND avg_qty_sold >= 1 THEN sku_id END) AS efficient_movers,
  SUM(sold_items) AS sold_items,
  SUM(gpv_eur) AS gpv_eur
FROM sps_efficiency_month
GROUP BY GROUPING SETS (...)
```

**Problemă:** Calculează `COUNT(DISTINCT)` direct, ceea ce NU este aditiv cross-warehouse.

#### După (flat_sps)

```sql
-- 3 CTE-uri separate după tip metrică

CTE A: sku_counts (COUNT(DISTINCT) fără warehouse)
├─ COUNT(DISTINCT sku) la nivel (supplier, lună, categorie) FĂRĂ warehouse_id
├─ Rezultat: numărări unice de SKU pe dimensiune

CTE B: efficiency_by_warehouse (metrici aditive CU warehouse)
├─ SUM(sold_items), SUM(gpv_eur), SUM(numerator_new_avail), SUM(denom_new_avail)
├─ NOU: weight_efficiency = SAFE_DIVIDE(...) * SUM(gpv_eur)
├─ Rezultat: metrici aditive la nivel (supplier, warehouse, lună)

CTE C: combined (JOIN A + B)
├─ LEFT JOIN sku_counts (A) cu efficiency_by_warehouse (B)
├─ sku_counts rămâne fix (nu se sumează)
├─ efficiency_by_warehouse sumată cross-warehouse
└─ Rezultat: combinație corectă de numărări + sume
```

###  📐 Modificări în formule de calcul

#### Câmp nou: `weight_efficiency`

**Definiție:**
```sql
weight_efficiency = (
  SUM(COUNT(DISTINCT efficient_skus)) OVER (PARTITION BY supplier, warehouse)
  / NULLIF(
      SUM(COUNT(DISTINCT efficient_or_qualified_skus)) OVER (...),
      0
  )
) * SUM(gpv_eur)
```

**Interpretare:**
- Numărător: Contul SKU-urilor eficiente
- Numitor: Contul SKU-urilor eligibile (eficiente + slow_movers + zero_movers calificați)
- Pondere: Înmulțit cu GPV pentru ponderare după volum

**Utilizare în Tableau:**
```
Formulă Tableau = SUM(weight_efficiency) / SUM(gpv_eur)
= Eficiență % ponderată după GPV la nivel agregat
```

### 🔄 Câmpuri care SCHIMBĂ definiție

| Câmp | Înainte (v5, direct) | După (v7, agregat) | Diferență |
|-------|-------------------|----------------------|-----------|
| `efficient_movers` | `COUNT(DISTINCT ... date_diff >= 90 AND avg_qty_sold >= 1)` | `COUNT(DISTINCT ... updated_sku_age >= 90 AND sku_efficiency = 'efficient_sku')` | Logică schimbată (AQS v5→v7) |
| `sku_listed` | `COUNT(DISTINCT sku_id)` | Precalculat în `sku_counts` CTE | Sursă schimbată |
| `sku_mature` | `COUNT(DISTINCT ... date_diff >= 90)` | `COUNT(DISTINCT ... updated_sku_age >= 90)` | Nume/logică |
| `sold_items` | `SUM(sold_items)` | `SUM(SUM(...) OVER warehouse)` | Acum via funcție fereastră |
| `gpv_eur` | `SUM(gpv_eur)` | `SUM(SUM(...) OVER warehouse)` | Acum via funcție fereastră |

### 🔍 Validare recomandată

```sql
-- Compară totaluri la nivel țară
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

-- Compară cu sps_efficiency v5 (dacă încă există)
SELECT
  'sps_efficiency (v5)' as source,
  global_entity_id,
  SUM(sku_listed) as total_skus,
  SUM(sku_mature) as total_mature,
  SUM(efficient_movers) as total_efficient,
  ROUND(SUM(gpv_eur) / NULLIF(SUM(sold_items), 0), 2) as efficiency_pct
FROM sps_efficiency  -- tabel vechi
GROUP BY global_entity_id;
```

### ✅ Concluzie

**REFACTORING IMPORTANT dar BINE FONDAT.**
- Separare COUNT(DISTINCT) vs SUM abordează agregarea corectă cross-warehouse
- `weight_efficiency` este metrică nouă (metodologia AQS v7)
- Totalurile pot diferi de v5 datorită schimbării metodologiei AQS (v5→v7)
- **Acțiune critică:** Validează cu echipa PFC dacă eficiență % se aliniază cu scorecard

---

## 📊 TABEL 4-7: sps_listed_sku, sps_days_payable, sps_price_index, sps_shrinkage, sps_line_rebate_metrics

**Rezumat rapid:** Aceste tabele urmează modelul (tabel_month → tabel_agregat)

| Tabel | Modificări | Criticitate | Acțiune |
|-------|---------|-----------|--------|
| **listed_sku** | FROM: listed_sku_month (fără modificări logică) | 🟢 SCĂZUTĂ | Verifică numărări linii |
| **days_payable** | FROM: days_payable_month (CTE-uri consolidate în _month) | 🟡 MEDIE | Validează formula DOH |
| **price_index** | FROM: price_index_month (CTE-uri consolidate) | 🟡 MEDIE | Validează median prețuri |
| **shrinkage** | FROM: shrinkage_month (modificări minore coloane finale) | 🟡 MEDIE | Validează spoilage_rate |
| **line_rebate_metrics** | FROM: line_rebate_metrics_month (nouă CTE agregată) | 🟡 MEDIE | Validează rebate_values |

### Detalii pe tabel

#### sps_listed_sku
- **Modificări:** 0 linii diff (complet identic)
- **Acțiune:** ✅ Procedează fără modificări

#### sps_days_payable
- **Modificări:** +16 linii
- **Motiv:** Probabil documentație îmbunătățită
- **Câmpuri finale:** payment_days, doh, dpo, stock_value_eur, cogs_monthly_eur, days_in_month, days_in_quarter
- **Validează:** `doh = SUM(stock_value_eur) / (SUM(cogs_monthly_eur) / MAX(days_in_month))` rămâne aceeași

#### sps_price_index
- **Modificări:** +8 linii
- **Posibile câmpuri noi:** Coloane derivate noi
- **Validează:** `median_price_index`, `price_index_numerator`, `price_index_weight` calculate corect

#### sps_shrinkage
- **Modificări:** +6 linii
- **Câmpuri finale:** spoilage_value_eur, spoilage_value_lc, retail_revenue_eur, retail_revenue_lc, spoilage_rate
- **Validează:** `spoilage_rate = spoilage_value / retail_revenue` menține logica

#### sps_line_rebate_metrics
- **Modificări:** -9 linii (simplificare)
- **Motiv:** CTE-uri consolidate în _month
- **Validează:** Calculele rebate mențin precizie

---

## 📊 TABEL 8: sps_financial_metrics

**Poziție în DAG:** Nivel 3 (depinde de: sps_customer_order, sps_financial_metrics_month)

| Aspect | Modificări |
|--------|---------|
| Linii | 168 → 167 (-1) |
| CTE-uri | Schimbare internă în _month (consolidare) |
| Câmpuri finale | Fără modificări semnificative |

**Acțiune:** ✅ Procedează. Modificări minime.

---

## 📊 TABEL 9: sps_purchase_order

**Poziție în DAG:** Nivel 3 (depinde de: sps_purchase_order_month)  
**Criticitate:** 🟡 **MEDIE** - Câmpuri debug adăugate

### Modificări detectate

| Modificare | Linii | Tip |
|--------|-------|------|
| Câmpuri debug | +4 | NOU |
| Linii totale | +13 | Modificări minore |

### Noi câmpuri debug adăugate

```sql
-- Câmpuri debug (fără modificări logică originală)
COUNT(DISTINCT(po_order_id)) AS total_po_orders,
COUNT(DISTINCT(CASE WHEN is_compliant_flag THEN po_order_id END)) AS total_compliant_po_orders,
COALESCE(SUM(total_received_qty_per_order), 0) AS total_received_qty_ALL,
COALESCE(SUM(total_demanded_qty_per_order), 0) AS total_demanded_qty_ALL
```

**Scop:**
- `total_po_orders`: Toate comenzile (pentru audit raport compliant)
- `total_compliant_po_orders`: Comenzi valide
- `total_received_qty_ALL`: Cantitate primită fără filtru status
- `total_demanded_qty_ALL`: Cantitate cerută fără filtru status

**Impact pe score_tableau:**
- Aceste 4 câmpuri moștenite de score_tableau
- Utile pentru auditarea calculelor fill_rate și OTD

### ✅ Concluzie

**Modificări sigure.** Sunt câmpuri informative, nu alterează metrici principale.

---

## 📊 TABEL 10: sps_score_tableau (FINAL - CONCENTRATOR)

**Poziție în DAG:** Nivel 4 (FINAL - depinde de TOATE)  
**Criticitate:** 🔴 **FOARTE RIDICATĂ** - Schimbare arhitecturală + moștenește schimbări de la toate

### 🏗️ SCHIMBARE ARHITECTURALĂ FUNDAMENTALĂ

#### Înainte (sps_originals)

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

**Problemă:** Baza este `sps_purchase_order`. Dacă o metrică (ex: eficiență) nu are date pentru o cheie, acele linii se pierd.

#### După (flat_sps)

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

**Avantaj:** Garantează că TOATE cheile unice (din orice tabel) sunt prezente.

###  📋 COLOANE FINALE COMPLETE

#### Coloane de dimensiuni (moștenite de la all_keys)
```
global_entity_id          -- Marketplace/țară
time_period               -- Lună sau trimestru
brand_sup                 -- ID Supplier sau brand
entity_key                -- Cheie ierarhică (principal/diviziune/brand/categorie)
division_type             -- Tip: principal, division, brand_owner, brand_name, total
supplier_level            -- Nivel granularitate
time_granularity          -- Lunar sau Trimestrial
```

#### Coloane price_index (moștenite din sps_price_index)
```
median_price_index        -- Mediana indicelui de preț
price_index_numerator     -- SUM(median_bp_index * sku_gpv_eur)
price_index_weight        -- SUM(sku_gpv_eur)
```

#### Coloane days_payable (moștenite din sps_days_payable)
```
payment_days              -- DPO (zile de plată)
doh                       -- Days on hand (inventar)
dpo                       -- Days payable outstanding
stock_value_eur           -- Valoare stoc în EUR
cogs_monthly_eur          -- COGS lunar în EUR
days_in_month             -- Zile din lună
days_in_quarter           -- Zile din trimestru
```

#### Coloane financial_metrics (via * EXCEPT)
```
(toate cu excepția dimensiunilor de bază)
Include: Net_Sales, Gross_Margin, COGS, etc.
```

#### Coloane line_rebate_metrics (via * EXCEPT)
```
(toate cu excepția dimensiunilor și net_purchase)
Include: rebate_values, rebate_pct, etc.
```

#### Coloane efficiency (AQS v7 - NORI DEFINIȚII)

**Numitori (universuri):**
```
sku_listed                -- Cont SKU-uri listate (is_listed = TRUE)
sku_mature                -- SKU-uri cu updated_sku_age >= 90 (univers eficiență)
sku_new                   -- SKU-uri cu updated_sku_age <= 30
sku_probation             -- SKU-uri cu updated_sku_age 31-89 (în ramp-up)
```

**Numărători (SKU-uri eficiente):**
```
efficient_movers          -- SKU-uri mature care vând (sku_efficiency = 'efficient_sku')
                          -- Notă: NU folosi eficient/matur ca ratio. Folosi weight_efficiency în schimb.
```

**Ingrediente disponibilitate (aditive):**
```
numerator_new_avail       -- SUM(available_events_weightage * sales_forecast_qty_corr)
denom_new_avail           -- SUM(total_events_weightage * sales_forecast_qty_corr)
                          -- Formulă Tableau: SUM(numerator)/SUM(denom) = procent disponibilitate ponderată
```

**Ingrediente eficiență ponderată (AQS v7):**
```
weight_efficiency         -- Numărător eficiență ponderat GPV
gpv_eur                   -- Numitor GPV
                          -- Formulă Tableau: SUM(weight_efficiency)/SUM(gpv_eur) = procent eficiență
listed_skus_efficiency    -- Alias de sku_listed (pentru claritate în Tableau)
```

#### Coloane listed_sku
```
listed_skus               -- Cont SKU-uri listate
```

#### Coloane shrinkage
```
spoilage_value_eur        -- Valoare risipă în EUR
spoilage_value_lc         -- Valoare risipă în monedă locală
retail_revenue_eur        -- Venituri amănunt în EUR
retail_revenue_lc         -- Venituri amănunt în monedă locală
spoilage_rate             -- Procent risipă (%)
```

#### Coloane delivery_costs
```
delivery_cost_eur         -- Cost livrare în EUR
delivery_cost_local       -- Cost livrare în monedă locală
```

#### Coloane purchase_order (incluzând câmpuri debug)
```
on_time_orders            -- Comenzi la timp și compliant
total_received_qty_per_po_order  -- Cantitate primită pe comandă
total_demanded_qty_per_po_order  -- Cantitate cerută pe comandă
total_cancelled_po_orders -- Comenzi anulate
total_non_cancelled__po_orders -- Comenzi neanulave
fill_rate                 -- Procent completare
otd                       -- Livrare la timp
supplier_non_fulfilled_order_qty -- Cantitate necompletată de supplier
total_po_orders           -- 🆕 DEBUG: Total comenzi
total_compliant_po_orders -- 🆕 DEBUG: Comenzi compliant
total_received_qty_ALL    -- 🆕 DEBUG: Cantitate primită total (fără filtru)
total_demanded_qty_ALL    -- 🆕 DEBUG: Cantitate cerută total (fără filtru)
```

### 🔴 MODIFICĂRI CRITICE ÎN SCORE_TABLEAU

#### 1. Schimbare arhitecturală (all_keys UNION)
- **Înainte:** Putea pierde linii dacă o metrică nu avea date
- **După:** Garantează acoperire din toate cheile
- **Impact:** Mai multe linii în rezultat, în special în perioade fără date eficiență

#### 2. Schimbare în formule eficiență (AQS v5→v7)
- **Înainte:** Numărări directe fără pondere GPV
- **După:** weight_efficiency ponderat (metrică nouă)
- **Impact Tableau:** Trebuie schimbare de la `efficient_movers / sku_mature` la `SUM(weight_efficiency) / SUM(gpv_eur)`

#### 3. Ingrediente disponibilitate noi
- **Înainte:** Câmp precalculat `new_availability` (nu aditiv)
- **După:** Descompus în `numerator_new_avail` și `denom_new_avail` (aditiv)
- **Impact:** Schimbare în cum Tableau calculează procent disponibilitate

#### 4. Câmpuri debug adăugate
- **4 câmpuri noi** din purchase_order pentru audit
- **Impact:** Utile pentru validarea fill_rate și OTD, dar nu afectează metrici principale

### 🔍 Validare recomandată (SQL)

```sql
-- 1. Compară numărări linii (înainte vs după arhitectură)
SELECT
  'Arhitectură UNION (flat_sps)' as tip,
  COUNT(*) as linii_totale,
  COUNT(DISTINCT global_entity_id) as tari,
  COUNT(DISTINCT (global_entity_id, time_period, brand_sup)) as chei_unice
FROM flat_sps_score_tableau

UNION ALL

SELECT
  'Arhitectură JOIN (sps_originals)',
  COUNT(*),
  COUNT(DISTINCT global_entity_id),
  COUNT(DISTINCT (global_entity_id, time_period, brand_sup))
FROM sps_score_tableau;

-- 2. Validează schimbare eficiență (v5 vs v7)
SELECT
  global_entity_id,
  time_period,
  -- Formulă v5: efficient_movers / sku_mature
  ROUND(SAFE_DIVIDE(SUM(efficient_movers), NULLIF(SUM(sku_mature), 0)) * 100, 2) as v5_efficiency_pct,
  -- Formulă v7: weight_efficiency / gpv_eur
  ROUND(SAFE_DIVIDE(SUM(weight_efficiency), NULLIF(SUM(gpv_eur), 0)) * 100, 2) as v7_efficiency_pct,
  COUNT(*) as linii
FROM flat_sps_score_tableau
WHERE CAST(time_period AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY 1, 2
HAVING COUNT(*) > 0
ORDER BY global_entity_id, time_period DESC;

-- 3. Validează ingrediente disponibilitate
SELECT
  global_entity_id,
  SUM(numerator_new_avail) / NULLIF(SUM(denom_new_avail), 0) as procent_disponibilitate_ponderat,
  COUNT(DISTINCT (time_period, brand_sup)) as combinatii_chei
FROM flat_sps_score_tableau
WHERE denom_new_avail IS NOT NULL AND denom_new_avail > 0
GROUP BY global_entity_id;
```

### ✅ Concluzie

**SCHIMBARE INTEGRALĂ și FOARTE IMPORTANTĂ.**
- Arhitectura UNION garantează acoperire completă
- AQS v5→v7 introduce metrică ponderată (`weight_efficiency`)
- Tablourile Tableau TREBUIE să actualizeze formule eficiență
- Câmpurile debug utile pentru audit

**Acțiuni critice:**
1. ✅ Validează numărări linii (așteptat: mai multe linii decât înainte)
2. ✅ Actualizează Tableau: eficiență = SUM(weight_efficiency) / SUM(gpv_eur)
3. ✅ Validează disponibilitate: folosește SUM(numerator) / SUM(denom), nu new_availability precalculat
4. ✅ Comunică stakeholders schimbare în procent eficiență (acum ponderat)

---

## 📌 REZUMAT VALIDĂRI CRITICE

| Tabel | Validare | Interogare SQL | Proprietar |
|-------|-----------|-----------|-------|
| **efficiency_month** | Categorizare AQS v5→v2 | Vezi secțiunea "AQS v5 vs AQS v7" | DH |
| **efficiency** | Totaluri weight_efficiency | Vezi secțiunea "Validare recomandată" | DH + PFC |
| **score_tableau** | Comparație numărări linii | Vezi secțiunea "Validare SQL" | Analytics |
| **score_tableau** | Procent eficiență (v5 vs v7) | Vezi secțiunea "Validare SQL" | Analytics |
| **score_tableau** | Ingrediente disponibilitate | Vezi secțiunea "Validare SQL" | Analytics |

---

## 📚 REFERINȚE

- **Documentație AQS:** [Link la wiki/docs dacă există]
- **Modificări Tableau:** Actualizează datasource cu coloane noi
- **Echipe responsabile:** 
  - DH (Data Warehouse): Validare date
  - PFC (Product Financial Control): Validare metrici
  - Analytics: Validare Tableau

---

**Document generat:** 2026-04-07  
**Versiune:** RO (Română)  

