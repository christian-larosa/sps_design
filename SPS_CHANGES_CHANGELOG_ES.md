# 📚 MANUAL DE CAMBIOS SPS | Versión España (ES)

**Documento maestro de cambios: sps_originals → flat_sps_***  
**Audiencia:** Senior Data Analysts  
**Formato:** Por tabla, en orden del DAG  
**Última actualización:** 2026-04-07

---

## 🗂️ ORDEN DEL DAG

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

   └─ sps_score_tableau (FINAL - depende de TODOS)
```

---

## 📊 TABLA 1: sps_product (BASE)

**Posición en DAG:** Raíz  
**Dependencias:** Ninguna (tabla base)

### 📈 Cambios detectados

| Aspecto | Antes | Después | Impacto |
|---------|-------|---------|--------|
| Líneas | 808 | 806 | -2 líneas (limpieza) |
| CTEs | 1 (simple) | 1 (simple) | Sin cambios |
| FROM clauses | Múltiples tablas internas | Múltiples tablas internas | Sin cambios significativos |

### ✅ Verificación

```bash
# Diferencia total
diff sps_product.sql flat_sps_product.sql | wc -l
# Output: 113 cambios (mayormente hardcodes de parámetros/dataset)
```

### 📝 Conclusión

**No hay cambios sustanciales en lógica o campos.** Solo:
- Hardcodes de proyecto/dataset (`{{ params.project_id }}` → `dh-darkstores-live`)
- Posibles cambios menores de formato/espacios

**Recomendación:** Proceder como base confiable. Validar que los mismos SKUs existan.

---

## 📊 TABLA 2: sps_efficiency_month

**Posición en DAG:** Nivel 2 (depende de: sps_product, AQS data)  
**Dependencias:** sps_product, sku_efficiency_detail_v2  
**Criticidad:** 🔴 **MUY ALTA** - Cambio de metodología AQS

### 🔴 CAMBIO CRÍTICO: AQS v5 → AQS v2 (que implementa AQS v7)

#### Cambio de fuente de datos

| Parámetro | sps_originals (AQS v5) | flat_sps (AQS v2) | Implicación |
|-----------|------------------------|-------------------|------------|
| **Tabla** | `_aqs_v5_sku_efficiency_detail` | `sku_efficiency_detail_v2` | Nueva metodología |
| **Dataset** | `{{ params.dataset.rl }}` | `fulfillment-dwh-production.rl_dmart` | Cambio de ubicación |
| **Partición** | `partition_month` | `partition_month` | Igual (compatible) |

#### Cambios en campos - COMPARATIVA DIRECTA

| Campo | Antes (v5) | Después (v2/v7) | Tipo | Impacto |
|-------|-----------|-----------------|------|--------|
| `sku_efficiency` | ❌ NO EXISTE | ✅ NUEVO (ENUM) | NUEVO | **CRÍTICO** - Categorización predefinida |
| `updated_sku_age` | ❌ NO EXISTE | ✅ NUEVO (INT) | NUEVO | Reemplaza `date_diff` |
| `available_hours` | ❌ NO EXISTE | ✅ NUEVO | NUEVO | Nueva métrica de disponibilidad |
| `potential_hours` | ❌ NO EXISTE | ✅ NUEVO | NUEVO | Nueva métrica de disponibilidad |
| `numerator_new_avail` | ❌ NO EXISTE | ✅ NUEVO | NUEVO | Ingrediente para disponibilidad ponderada |
| `denom_new_avail` | ❌ NO EXISTE | ✅ NUEVO | NUEVO | Ingrediente para disponibilidad ponderada |
| `sku_status` | ❌ NO EXISTE | ✅ NUEVO | NUEVO | Estado del SKU |
| `is_listed` | ❌ NO EXISTE | ✅ NUEVO (BOOL) | NUEVO | Indicador de listado |
| `date_diff` | ✅ EXISTE | ❌ REMOVIDO | REMOVIDO | Reemplazado por `updated_sku_age` |
| `avg_qty_sold` | ✅ EXISTE | ❌ REMOVIDO | REMOVIDO | Ahora viene como categoría en `sku_efficiency` |
| `new_availability` | ✅ EXISTE | ❌ REMOVIDO (pero vive en ingredientes) | REMOVIDO | Descompuesta en numerator/denom |

### 📐 Cambio de lógica - AQS v5 vs AQS v7

**AQS v5 (antes):**
```sql
-- Categorización manual basada en lógica
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

**AQS v2/v7 (después):**
```sql
-- Campo precalculado desde fuente (sku_efficiency_detail_v2)
e.sku_efficiency  -- ENUM: 'efficient_sku', 'zero_mover', 'slow_mover', ...
```

### ⚠️ Implicaciones para downstream (sps_efficiency)

```sql
-- ANTES (v5):
COUNT(DISTINCT CASE WHEN date_diff >=90 AND (avg_qty_sold >= 1) THEN sku_id END) AS efficient_movers

-- DESPUÉS (v2/v7):
COUNT(DISTINCT CASE WHEN updated_sku_age >= 90 AND sku_efficiency = 'efficient_sku' THEN sku_id END) AS efficient_movers
```

**Diferencia conceptual:**
- `date_diff` = días desde primer listado (simple)
- `updated_sku_age` = días con "reset logic" (si hay cambios de warehouse, recalcula) → **Más preciso**

### 🔍 Validación recomendada

```sql
-- Verificar que ambas metodologías producen recuentos similares
SELECT
  COUNT(DISTINCT CASE WHEN updated_sku_age >= 90 AND sku_efficiency = 'efficient_sku' THEN sku_id END) as v7_efficient,
  COUNT(DISTINCT CASE WHEN date_diff >= 90 AND avg_qty_sold >= 1 THEN sku_id END) as v5_efficient,
  (COUNT(*) OVER () - COUNT(DISTINCT CASE WHEN sku_efficiency IN ('efficient_sku', 'zero_mover', 'slow_mover') THEN sku_id END)) as unmapped_skus
FROM flat_sps_efficiency_month
WHERE CAST(month AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY global_entity_id, supplier_id;
```

### ✅ Conclusión

**CAMBIO METODOLÓGICO IMPORTANTE pero COMPATIBLE.**  
- AQS v7 es más preciso (`updated_sku_age` con reset logic)
- Categorías (`sku_efficiency`) vienen precalculadas (menos error manual)
- Downstream (sps_efficiency) **sí depende** de estos cambios → **validar totales**

---

## 📊 TABLA 3: sps_efficiency

**Posición en DAG:** Nivel 3 (depende de: sps_efficiency_month)  
**Dependencias:** sps_efficiency_month  
**Criticidad:** 🔴 **MUY ALTA** - Refactor arquitectónico + fórmula nueva

### 🏗️ CAMBIO ARQUITECTÓNICO: 1 CTE → 3 CTEs

#### Antes (sps_originals)

```sql
-- Estructura simple: 1 SELECT directo + GROUPING SETS
SELECT
  global_entity_id,
  CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING) ELSE quarter_year END AS time_period,
  -- ... dimensiones ...
  COUNT(DISTINCT sku_id) AS sku_listed,
  COUNT(DISTINCT CASE WHEN date_diff >= 90 THEN sku_id END) AS sku_mature,
  COUNT(DISTINCT CASE WHEN ... AND avg_qty_sold >= 1 THEN sku_id END) AS efficient_movers,
  SUM(sold_items) AS sold_items,
  SUM(gpv_eur) AS gpv_eur
FROM sps_efficiency_month
GROUP BY GROUPING SETS (...)
```

**Problema:** Calcula `COUNT(DISTINCT)` directamente, que NO es aditivo cross-warehouse.

#### Después (flat_sps)

```sql
-- 3 CTEs separadas por tipo de métrica

CTE A: sku_counts (COUNT(DISTINCT) sin warehouse)
├─ COUNT(DISTINCT sku) a nivel (supplier, mes, categoría) SIN warehouse_id
├─ Resultado: conteos únicos de SKU por dimensión

CTE B: efficiency_by_warehouse (métricas aditivas CON warehouse)
├─ SUM(sold_items), SUM(gpv_eur), SUM(numerator_new_avail), SUM(denom_new_avail)
├─ NUEVO: weight_efficiency = SAFE_DIVIDE(...) * SUM(gpv_eur)
├─ Resultado: métricas aditivas a nivel (supplier, warehouse, mes)

CTE C: combined (JOIN A + B)
├─ LEFT JOIN sku_counts (A) con efficiency_by_warehouse (B)
├─ sku_counts se mantiene fijo (no se suma)
├─ efficiency_by_warehouse se suma cross-warehouse
└─ Resultado: combinación correcta de conteos + sumas
```

###  📐 Cambios en fórmulas de cálculo

#### Campo nuevo: `weight_efficiency`

**Definición:**
```sql
weight_efficiency = (
  SUM(COUNT(DISTINCT efficient_skus)) OVER (PARTITION BY supplier, warehouse)
  / NULLIF(
      SUM(COUNT(DISTINCT efficient_or_qualified_skus)) OVER (...),
      0
  )
) * SUM(gpv_eur)
```

**Interpretación:**
- Numerador: Conteo de SKUs eficientes
- Denominador: Conteo de SKUs elegibles (eficientes + slow_movers + zero_movers calificados)
- Peso: Multiplicado por GPV para ponderar por volumen

**Uso en Tableau:**
```
Tableau formula = SUM(weight_efficiency) / SUM(gpv_eur)
= Efficiency % ponderado por GPV a nivel agregado
```

### 🔄 Campos que CAMBIAN de definición

| Campo | Antes (v5, directo) | Después (v7, agregado) | Diferencia |
|-------|-------------------|----------------------|-----------|
| `efficient_movers` | `COUNT(DISTINCT ... date_diff >= 90 AND avg_qty_sold >= 1)` | `COUNT(DISTINCT ... updated_sku_age >= 90 AND sku_efficiency = 'efficient_sku')` | Lógica cambia (AQS v5→v7) |
| `sku_listed` | `COUNT(DISTINCT sku_id)` | Precalculado en `sku_counts` CTE | Fuente cambia |
| `sku_mature` | `COUNT(DISTINCT ... date_diff >= 90)` | `COUNT(DISTINCT ... updated_sku_age >= 90)` | Name/lógica |
| `sold_items` | `SUM(sold_items)` | `SUM(SUM(...) OVER warehouse)` | Ahora vía window function |
| `gpv_eur` | `SUM(gpv_eur)` | `SUM(SUM(...) OVER warehouse)` | Ahora vía window function |

### 🔍 Validación recomendada

```sql
-- Comparar totales a nivel country
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

-- Comparar con sps_efficiency v5 (si aún existe)
SELECT
  'sps_efficiency (v5)' as source,
  global_entity_id,
  SUM(sku_listed) as total_skus,
  SUM(sku_mature) as total_mature,
  SUM(efficient_movers) as total_efficient,
  ROUND(SUM(gpv_eur) / NULLIF(SUM(sold_items), 0), 2) as efficiency_pct
FROM sps_efficiency  -- tabla antigua
GROUP BY global_entity_id;
```

### ✅ Conclusión

**REFACTOR IMPORTANTE pero FUNDAMENTADO.**
- Separación de COUNT(DISTINCT) vs SUM responde a correcta agregación cross-warehouse
- `weight_efficiency` es nueva métrica (AQS v7 methodology)
- Totales pueden diferir de v5 debido a cambio de metodología AQS (v5→v7)
- **Acción crítica:** Validar con equipo de PFC si efficiency % se alinea con scorecard

---

## 📊 TABLA 4-7: sps_listed_sku, sps_days_payable, sps_price_index, sps_shrinkage, sps_line_rebate_metrics

**Resumen rápido:** Estas tablas siguen el patrón (tabla_month → tabla_aggregated)

| Tabla | Cambios | Criticidad | Acción |
|-------|---------|-----------|--------|
| **listed_sku** | FROM: listed_sku_month (sin cambios en lógica) | 🟢 BAJA | Verificar row counts |
| **days_payable** | FROM: days_payable_month (CTEs consolidadas en _month) | 🟡 MEDIA | Validar DOH fórmula |
| **price_index** | FROM: price_index_month (CTEs consolidadas) | 🟡 MEDIA | Validar mediana de precios |
| **shrinkage** | FROM: shrinkage_month (cambios menores en columnas finales) | 🟡 MEDIA | Validar spoilage_rate |
| **line_rebate_metrics** | FROM: line_rebate_metrics_month (nueva CTE agregada) | 🟡 MEDIA | Validar rebate_values |

### Detalles por tabla

#### sps_listed_sku
- **Cambios:** 0 líneas diff (completamente idéntico)
- **Acción:** ✅ Proceder sin cambios

#### sps_days_payable
- **Cambios:** +16 líneas
- **Motivo:** Probablemente documentación mejorada
- **Campos finales:** payment_days, doh, dpo, stock_value_eur, cogs_monthly_eur, days_in_month, days_in_quarter
- **Validar:** `doh = SUM(stock_value_eur) / (SUM(cogs_monthly_eur) / MAX(days_in_month))` sigue igual

#### sps_price_index
- **Cambios:** +8 líneas
- **Campos nuevos posibles:** Nuevas columnas derivadas
- **Validar:** `median_price_index`, `price_index_numerator`, `price_index_weight` se calculan correctamente

#### sps_shrinkage
- **Cambios:** +6 líneas
- **Campos finales:** spoilage_value_eur, spoilage_value_lc, retail_revenue_eur, retail_revenue_lc, spoilage_rate
- **Validar:** `spoilage_rate = spoilage_value / retail_revenue` mantiene lógica

#### sps_line_rebate_metrics
- **Cambios:** -9 líneas (simplificación)
- **Motivo:** CTEs consolidadas en _month
- **Validar:** Rebate calculations mantienen precisión

---

## 📊 TABLA 8: sps_financial_metrics

**Posición en DAG:** Nivel 3 (depende de: sps_customer_order, sps_financial_metrics_month)

| Aspecto | Cambios |
|---------|---------|
| Líneas | 168 → 167 (-1) |
| CTEs | Cambio interno en _month (consolidación) |
| Campos finales | Sin cambios significativos |

**Acción:** ✅ Proceder. Cambios mínimos.

---

## 📊 TABLA 9: sps_purchase_order

**Posición en DAG:** Nivel 3 (depende de: sps_purchase_order_month)  
**Criticidad:** 🟡 **MEDIA** - Campos de debugging agregados

### Cambios detectados

| Cambio | Líneas | Tipo |
|--------|--------|------|
| Campos de debugging | +4 | NUEVO |
| Líneas totales | +13 | Cambios menores |

### Campos nuevos agregados

```sql
-- Debug fields (sin cambios en lógica original)
COUNT(DISTINCT(po_order_id)) AS total_po_orders,
COUNT(DISTINCT(CASE WHEN is_compliant_flag THEN po_order_id END)) AS total_compliant_po_orders,
COALESCE(SUM(total_received_qty_per_order), 0) AS total_received_qty_ALL,
COALESCE(SUM(total_demanded_qty_per_order), 0) AS total_demanded_qty_ALL
```

**Propósito:**
- `total_po_orders`: Todas las órdenes (para auditar compliant ratio)
- `total_compliant_po_orders`: Órdenes válidas
- `total_received_qty_ALL`: Cantidad recibida sin filtro de status
- `total_demanded_qty_ALL`: Cantidad demandada sin filtro de status

**Impacto en score_tableau:**
- Estos 4 campos se heredan a score_tableau
- Útiles para auditoría de cálculos de fill_rate y OTD

### ✅ Conclusión

**Cambios seguros.** Son campos informativos, no alteran métricas principales.

---

## 📊 TABLA 10: sps_score_tableau (FINAL - CONCENTRADOR)

**Posición en DAG:** Nivel 4 (ÚLTIMO - depende de TODOS)  
**Criticidad:** 🔴 **MUY ALTA** - Cambio arquitectónico + hereda cambios de todos

### 🏗️ CAMBIO ARQUITECTÓNICO FUNDAMENTAL

#### Antes (sps_originals)

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

**Problema:** Base es `sps_purchase_order`. Si una métrica (ej: efficiency) no tiene datos para una key, se pierden esas filas.

#### Después (flat_sps)

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

**Ventaja:** Garantiza que TODAS las keys únicas (de cualquier tabla) están presentes.

###  📋 COLUMNAS FINALES COMPLETAS

#### Columnas de dimensiones (heredadas de all_keys)
```
global_entity_id          -- Marketplace/país
time_period               -- Mes o trimestre
brand_sup                 -- Supplier ID o brand
entity_key                -- Clave jerárquica (principal/division/brand/category)
division_type             -- Tipo: principal, division, brand_owner, brand_name, total
supplier_level            -- Nivel de granularidad
time_granularity          -- Monthly o Quarterly
```

#### Columnas de price_index (heredadas de sps_price_index)
```
median_price_index        -- Mediana del índice de precios
price_index_numerator     -- SUM(median_bp_index * sku_gpv_eur)
price_index_weight        -- SUM(sku_gpv_eur)
```

#### Columnas de days_payable (heredadas de sps_days_payable)
```
payment_days              -- DPO (días de pago)
doh                       -- Days on hand (inventario)
dpo                       -- Days payable outstanding
stock_value_eur           -- Valor del stock en EUR
cogs_monthly_eur          -- COGS mensual en EUR
days_in_month             -- Días del mes
days_in_quarter           -- Días del trimestre
```

#### Columnas de financial_metrics (via * EXCEPT)
```
(todos excepto dimensiones básicas)
Incluye: Net_Sales, Gross_Margin, COGS, etc.
```

#### Columnas de line_rebate_metrics (via * EXCEPT)
```
(todos excepto dimensiones y net_purchase)
Incluye: rebate_values, rebate_pct, etc.
```

#### Columnas de efficiency (AQS v7 - NUEVAS DEFINICIONES)

**Denominadores (universos):**
```
sku_listed                -- Conteo de SKUs listados (is_listed = TRUE)
sku_mature                -- SKUs con updated_sku_age >= 90 (universo de eficiencia)
sku_new                   -- SKUs con updated_sku_age <= 30
sku_probation             -- SKUs con updated_sku_age 31-89 (en ramp-up)
```

**Numeradores (SKUs eficientes):**
```
efficient_movers          -- SKUs mature que venden (sku_efficiency = 'efficient_sku')
                          -- Nota: NO se usa efficient/mature como ratio. Usar weight_efficiency en su lugar.
```

**Ingredientes de disponibilidad (aditivos):**
```
numerator_new_avail       -- SUM(available_events_weightage * sales_forecast_qty_corr)
denom_new_avail           -- SUM(total_events_weightage * sales_forecast_qty_corr)
                          -- Tableau formula: SUM(numerator)/SUM(denom) = weighted availability %
```

**Ingredientes de eficiencia ponderada (AQS v7):**
```
weight_efficiency         -- GPV-weighted efficiency numerator
gpv_eur                   -- GPV denominator
                          -- Tableau formula: SUM(weight_efficiency)/SUM(gpv_eur) = efficiency %
listed_skus_efficiency    -- Alias de sku_listed (para claridad en Tableau)
```

#### Columnas de listed_sku
```
listed_skus               -- Conteo de SKUs listados
```

#### Columnas de shrinkage
```
spoilage_value_eur        -- Valor del desperdicio en EUR
spoilage_value_lc         -- Valor del desperdicio en moneda local
retail_revenue_eur        -- Ingresos minoristas en EUR
retail_revenue_lc         -- Ingresos minoristas en moneda local
spoilage_rate             -- Tasa de desperdicio (%)
```

#### Columnas de delivery_costs
```
delivery_cost_eur         -- Costo de entrega en EUR
delivery_cost_local       -- Costo de entrega en moneda local
```

#### Columnas de purchase_order (incluyendo debug fields)
```
on_time_orders            -- Órdenes a tiempo y compliant
total_received_qty_per_po_order  -- Cantidad recibida por orden
total_demanded_qty_per_po_order  -- Cantidad demandada por orden
total_cancelled_po_orders -- Órdenes canceladas
total_non_cancelled__po_orders -- Órdenes no canceladas
fill_rate                 -- Tasa de cumplimiento
otd                       -- On-time delivery
supplier_non_fulfilled_order_qty -- Cantidad no cumplida por supplier
total_po_orders           -- 🆕 DEBUG: Total de órdenes
total_compliant_po_orders -- 🆕 DEBUG: Órdenes compliant
total_received_qty_ALL    -- 🆕 DEBUG: Cantidad recibida total (sin filtro)
total_demanded_qty_ALL    -- 🆕 DEBUG: Cantidad demandada total (sin filtro)
```

### 🔴 CAMBIOS CRÍTICOS EN SCORE_TABLEAU

#### 1. Cambio arquitectónico (all_keys UNION)
- **Antes:** Podía perder filas si una métrica no tenía datos
- **Después:** Garantiza cobertura de todas las claves
- **Impacto:** Más filas en el resultado, especialmente en periodos sin eficiencia data

#### 2. Cambio en fórmulas de eficiencia (AQS v5→v7)
- **Antes:** Conteos directos sin ponderación GPV
- **Después:** weight_efficiency ponderado (nueva métrica)
- **Impacto en Tableau:** Debe cambiar de `efficient_movers / sku_mature` a `SUM(weight_efficiency) / SUM(gpv_eur)`

#### 3. Ingredientes nuevos de disponibilidad
- **Antes:** Campo `new_availability` precalculado (no aditivo)
- **Después:** Descompuesto en `numerator_new_avail` y `denom_new_avail` (aditivos)
- **Impacto:** Cambio en cómo Tableau calcula % de disponibilidad

#### 4. Debug fields agregados
- **4 nuevos campos** de purchase_order para auditoría
- **Impacto:** Útiles para validar fill_rate y OTD, pero no afectan métricas principales

### 🔍 Validación recomendada (SQL)

```sql
-- 1. Comparar row counts (antes vs después architecture)
SELECT
  'Arquitectura UNION (flat_sps)' as tipo,
  COUNT(*) as total_rows,
  COUNT(DISTINCT global_entity_id) as countries,
  COUNT(DISTINCT (global_entity_id, time_period, brand_sup)) as unique_keys
FROM flat_sps_score_tableau

UNION ALL

SELECT
  'Arquitectura JOIN (sps_originals)',
  COUNT(*),
  COUNT(DISTINCT global_entity_id),
  COUNT(DISTINCT (global_entity_id, time_period, brand_sup))
FROM sps_score_tableau;

-- 2. Validar cambio de eficiencia (v5 vs v7)
SELECT
  global_entity_id,
  time_period,
  -- Fórmula v5: efficient_movers / sku_mature
  ROUND(SAFE_DIVIDE(SUM(efficient_movers), NULLIF(SUM(sku_mature), 0)) * 100, 2) as v5_efficiency_pct,
  -- Fórmula v7: weight_efficiency / gpv_eur
  ROUND(SAFE_DIVIDE(SUM(weight_efficiency), NULLIF(SUM(gpv_eur), 0)) * 100, 2) as v7_efficiency_pct,
  COUNT(*) as rows
FROM flat_sps_score_tableau
WHERE CAST(time_period AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY 1, 2
HAVING COUNT(*) > 0
ORDER BY global_entity_id, time_period DESC;

-- 3. Validar ingredientes de disponibilidad
SELECT
  global_entity_id,
  SUM(numerator_new_avail) / NULLIF(SUM(denom_new_avail), 0) as weighted_availability_pct,
  COUNT(DISTINCT (time_period, brand_sup)) as key_combinations
FROM flat_sps_score_tableau
WHERE denom_new_avail IS NOT NULL AND denom_new_avail > 0
GROUP BY global_entity_id;
```

### ✅ Conclusión

**CAMBIO INTEGRAL y MUY IMPORTANTE.**
- Arquitectura UNION garantiza cobertura completa
- AQS v5→v7 introduce métrica ponderada (`weight_efficiency`)
- Tableau dashboards DEBEN actualizar fórmulas de eficiencia
- Debug fields útiles para auditoría

**Acciones críticas:**
1. ✅ Validar row counts (esperados: más filas que antes)
2. ✅ Actualizar Tableau: efficiency = SUM(weight_efficiency) / SUM(gpv_eur)
3. ✅ Validar disponibilidad: use SUM(numerator) / SUM(denom), no new_availability precalculado
4. ✅ Comunicar a stakeholders cambio en % de eficiencia (ahora ponderado)

---

## 📌 RESUMEN DE VALIDACIONES CRÍTICAS

| Tabla | Validación | SQL Query | Owner |
|-------|-----------|-----------|-------|
| **efficiency_month** | AQS v5→v2 categorización | Ver sección "AQS v5 vs AQS v7" | DH |
| **efficiency** | weight_efficiency totals | Ver sección "Validación recomendada" | DH + PFC |
| **score_tableau** | Row count comparison | Ver sección "Validación SQL" | Analytics |
| **score_tableau** | Efficiency % (v5 vs v7) | Ver sección "Validación SQL" | Analytics |
| **score_tableau** | Disponibilidad ingredientes | Ver sección "Validación SQL" | Analytics |

---

## 📚 REFERENCIAS

- **Documentación AQS:** [Link a wiki/docs si existe]
- **Cambios Tableau:** Actualizar datasource con nuevas columnas
- **Equipo responsable:** 
  - DH (Data Warehouse): Validación de datos
  - PFC (Product Financial Control): Validación de métricas
  - Analytics: Validación en Tableau

---

**Documento generado:** 2026-04-07  
**Versión:** ES (Español)  
**Próximas versiones:** EN (English), RO (Română)

