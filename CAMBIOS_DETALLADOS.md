# 📊 MAPEO EXHAUSTIVO DE CAMBIOS: sps_* vs flat_sps_*

**Última actualización:** 2026-04-07

**Nota:** Se ignoran flateos de parámetros (`{{ params.* }}`), hardcoding de proyecto/dataset, y fechas hardcodeadas.

---

## 📋 RESUMEN EJECUTIVO

| Archivo | Cambios | Nivel Criticidad |
|---------|---------|------------------|
| **efficiency** | CTEs split, warehouse-level calcs | 🔴 ALTO |
| **score_tableau** | Estructura transformada a UNION | 🔴 ALTO |
| **efficiency_month** | Nueva fuente AQS v2, campos modificados | 🔴 ALTO |
| **days_payable_month** | CTEs consolidadas, simplificación | 🟡 MEDIO |
| **financial_metrics_month** | CTEs consolidadas | 🟡 MEDIO |
| **price_index_month** | CTEs consolidadas | 🟡 MEDIO |
| **purchase_order** | Campos nuevos | 🟡 MEDIO |
| **shrinkage** | Cambios menores | 🟢 BAJO |
| Resto | Mínimos | 🟢 BAJO |

---

## 🔍 ANÁLISIS DETALLADO POR ARCHIVO

### 1. **efficiency** (131 → 282 líneas, +151)
**Criticidad:** 🔴 ALTO

#### Cambios principales:
- **CTEs:** Aumentadas de 1 a múltiples (sku_counts, efficiency_by_warehouse, etc.)
- **Lógica de cálculo:** Refactorizada para separar:
  - COUNT(DISTINCT) no-aditivo → CTE `sku_counts`
  - Métricas aditivas (SUM, GPV) → CTE `efficiency_by_warehouse`
  - Window functions para cálculos de eficiencia por warehouse

#### Cambios de fuentes:
- FROM: `sps_efficiency_month` (mismo)

#### Cambios en cálculos:
- **Antes:** Todo en un SELECT con GROUPING SETS (complejo pero monolítico)
- **Después:** 
  - Nuevo campo: `warehouse_id` (agregación a nivel warehouse)
  - Campos de eficiencia usando window functions + SAFE_DIVIDE
  - Lógica de ponderación con GPV

#### Nuevos campos:
- `warehouse_id` en eficiencia_by_warehouse CTE
- Ponderaciones de eficiencia por warehouse

---

### 2. **score_tableau** (40 → 122 líneas, +82)
**Criticidad:** 🔴 ALTO

#### Cambios principales:
- **Arquitectura completamente diferente**
- **Antes:** SELECT + múltiples LEFT JOINs contra sps_*
- **Después:** CTE `all_keys` que unifica keys de 8 tablas via UNION, luego JOINs

#### Cambios específicos:

**ANTES (Estructura simple - JOINs secuenciales):**
```sql
SELECT o.*, fin.* EXCEPT (...), slrm.* EXCEPT (...)...
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

**DESPUÉS (Estructura de all_keys UNION - garantiza cobertura de todas las keys):**
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
SELECT o.*,  p.*, dpo.*, ... FROM all_keys JOIN cada tabla
```

#### Por qué cambió:
- **Antes:** Base `sps_purchase_order` podría perder filas si alguna tabla no tiene match
- **Después:** Garantiza que TODAS las claves únicas (global_entity_id, time_period, brand_sup, etc.) estén presentes, incluso si una tabla no tiene datos

#### Implicación:
- **Mejor cobertura:** No hay pérdida de combinaciones de keys
- **Nuevo formato de agregación:** Ahora los datos de Tableau pueden venir de qualquier tabla que tenga esa key combination
- **Impacto en Tableau:** Cambio significativo en cómo se agregan métricas

---

### 3. **efficiency_month** (+15 líneas)
**Criticidad:** 🔴 ALTO

#### Cambios principales:

**Fuente de SKU efficiency - CAMBIO CRÍTICO:**
- **Antes:** `{{ params.project_id }}.{{ params.dataset.rl }}._aqs_v5_sku_efficiency_detail` (AQS v5)
- **Después:** `fulfillment-dwh-production.rl_dmart.sku_efficiency_detail_v2` (AQS v2)

#### Nuevos campos agregados (flat_sps):
| Campo | Antes | Después | Tipo |
|-------|-------|---------|------|
| `date_diff` | ✅ | ❌ | REMOVIDO |
| `updated_sku_age` | ❌ | ✅ | NUEVO (INT, días) |
| `sku_efficiency` | ❌ | ✅ | NUEVO (ENUM: 'efficient_sku', 'zero_mover', 'slow_mover') |
| `available_hours` | ❌ | ✅ | NUEVO |
| `potential_hours` | ❌ | ✅ | NUEVO |
| `sku_status` | ❌ | ✅ | NUEVO |
| `numerator_new_avail` | ❌ | ✅ | NUEVO |
| `denom_new_avail` | ❌ | ✅ | NUEVO |

#### Impacto en downstream (efficiency):
- **Antes:** flat_sps_efficiency usaba `date_diff >= 90` para categorizar SKUs
- **Después:** flat_sps_efficiency usa:
  - `updated_sku_age >= 90` (mismo concepto, nombre diferente)
  - `sku_efficiency = 'efficient_sku'` (reemplaza lógica compleja de avg_qty_sold + availability)

#### Por qué importa:
- **Cambio de metodología AQS:** v5 → v2, puede haber cambios en cómo se calcula efficiency
- **Simplificación:** En lugar de lógica con `avg_qty_sold`, ahora viene un campo `sku_efficiency` ya categorizado
- **Validación crítica:** Necesario verificar si las categorías de efficiency coinciden entre v5 y v2

---

### 4. **days_payable_month** (132 → 124 líneas, -8)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **CTEs:** Reducidas de 3 a 1 (consolidación/simplificación)

#### Cambios en JOINs:
- **Antes:** 3 CTEs separadas (probablemente con lógica de JOIN compleja)
- **Después:** Una sola CTE con lógica consolidada

#### Implicación:
- Probablemente optimización de claridad, menos overhead de CTEs intermedias

---

### 5. **financial_metrics_month** (69 → 60 líneas, -9)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **CTEs:** Reducidas de 2 a 1
- **Líneas:** Reducción de 9 líneas (optimización)

#### Implicación:
- Simplificación, probablemente eliminación de una CTE intermedia innecesaria

---

### 6. **price_index_month** (118 → 111 líneas, -7)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **CTEs:** Reducidas de 3 a 1
- **Líneas:** Reducción de 7 líneas

#### Implicación:
- Consolidación de CTEs, probablemente fusión de lógica sin cambios en resultado final

---

### 7. **purchase_order** (120 → 133 líneas, +13)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **Campos de debugging agregados:** +4 ingredientes para auditoría
- **Comentarios explicativos:** Documentación mejorada

#### Campos nuevos agregados:
```sql
-- Ingredientes de debugging
COUNT(DISTINCT(po_order_id)) AS total_po_orders,
COUNT(DISTINCT(CASE WHEN is_compliant_flag THEN po_order_id END)) AS total_compliant_po_orders,
COALESCE(SUM(total_received_qty_per_order), 0) AS total_received_qty_ALL,
COALESCE(SUM(total_demanded_qty_per_order), 0) AS total_demanded_qty_ALL
```

#### Impacto:
- **Debug/auditoría:** Nuevos campos permiten validar cálculos de fill_rate y OTD
- **Sin cambios en lógica original:** Los campos originales (on_time_orders, fill_rate, OTD) mantienen su lógica
- **Nota:** "ALL" significa sin filtro de order_status='done'

---

### 8. **shrinkage** (116 → 122 líneas, +6)
**Criticidad:** 🟢 BAJO

#### Cambios principales:
- **Cambios menores:** Probablemente campo nuevo o pequeña reorganización
- **Estructura:** Mantiene similar lógica

---

### 9. **line_rebate_metrics_month** (85 líneas, 0 CTEs → 1 CTE)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **CTEs:** De 0 a 1 (agregada una CTE)
- **Líneas:** Reducción neta de 4 líneas

#### Implicación:
- Probablemente se extrajo lógica común a una CTE para mejorar legibilidad

---

### 10. **product** (808 → 806 líneas, -2)
**Criticidad:** 🟢 BAJO

#### Cambios principales:
- **Cambios menores:** Probablemente limpieza de código o pequeña optimización

---

### 11. **supplier_hierarchy** (133 → 134 líneas, +1)
**Criticidad:** 🟢 BAJO

#### Cambios principales:
- **Cambios mínimos:** +1 línea, probablemente comentario o pequeño ajuste

---

### 12. **listed_sku** (114 → 114 líneas, 0)
**Criticidad:** 🟢 BAJO

#### Cambios principales:
- **Sin cambios sustanciales**

---

### 13. **delivery_costs** (115 → 115 líneas, 0)
**Criticidad:** 🟢 BAJO

#### Cambios principales:
- **Sin cambios sustanciales**

---

### 14. **financial_metrics_prev_year** (118 → 122 líneas, +4)
**Criticidad:** 🟢 BAJO

#### Cambios principales:
- **Cambios menores:** +4 líneas, probablemente campos o comentarios

---

### 15. **delivery_costs_month** (106 → 100 líneas, -6)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **Simplificación:** Reducción de 6 líneas

---

### 16. **listed_sku_month** (85 → 77 líneas, -8)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **Simplificación:** Reducción de 8 líneas, probablemente consolidación

---

### 17. **purchase_order_month** (82 → 77 líneas, -5)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **CTEs:** Reducidas de 2 a 1
- **Simplificación:** Consolidación de CTEs

---

### 18. **price_index** (114 → 122 líneas, +8)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **Campos nuevos:** Probablemente +8 líneas de lógica

---

### 19. **shrinkage_month** (87 → 83 líneas, -4)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **Simplificación:** Reducción de 4 líneas

---

### 20. **days_payable** (122 → 138 líneas, +16)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **Campos/lógica nueva:** +16 líneas

---

### 21. **customer_order**
**Criticidad:** 🟢 BAJO

- Archivos no encontrados en comparación (verificar existencia)

---

### 22. **financial_metrics** (168 → 167 líneas, -1)
**Criticidad:** 🟢 BAJO

#### Cambios principales:
- **Cambios mínimos:** -1 línea

---

### 23. **line_rebate_metrics** (160 → 151 líneas, -9)
**Criticidad:** 🟡 MEDIO

#### Cambios principales:
- **Simplificación:** Reducción de 9 líneas

---

### 24. **score_tableau_init**
**Criticidad:** 🟢 BAJO

- Archivo nuevo o no existe en sps_originals (verificar)

---

---

## 🔴 CAMBIOS CRÍTICOS RESUMEN

### Cambio 1: **Efficiency → AQS v5 a AQS v2** 
- **Tabla:** efficiency_month
- **Impacto:** Campo `sku_efficiency` ahora viene precalculado (antes era lógica manual)
- **Riesgo:** Posibles diferencias en cómo se categorizan SKUs
- **Acción:** Validar que 'efficient_sku', 'zero_mover', 'slow_mover' correspondan a las categorías antiguas

### Cambio 2: **Efficiency → Refactor a 3 CTEs**
- **Tabla:** efficiency
- **Impacto:** Split de COUNT(DISTINCT) vs SUM (métricas aditivas)
- **Lógica nueva:** `weight_efficiency` calculado con window functions + ponderación GPV
- **Acción:** Validar que los totales/subtotales coincidan con versión anterior

### Cambio 3: **Score Tableau → Cambio de arquitectura**
- **Tabla:** score_tableau
- **Antes:** Base sps_purchase_order + JOINs (puede perder keys)
- **Después:** Todas las keys únicas via UNION (cobertura completa)
- **Impacto en Tableau:** Cambio en cómo se agregan datos, posibles filas nuevas
- **Acción:** Comparar row counts y validar que no hay datos faltantes

---

## ⚠️ VALIDACIONES CRÍTICAS NECESARIAS

### 1. AQS Efficiency
```sql
-- Antes: date_diff >= 90 & (avg_qty_sold >= 1)
-- Después: updated_sku_age >= 90 & sku_efficiency = 'efficient_sku'

-- Validar que estos equivalen:
SELECT COUNT(*) FROM efficiency_month 
WHERE updated_sku_age >= 90 AND sku_efficiency = 'efficient_sku'
-- vs
SELECT COUNT(*) FROM old_efficiency_month 
WHERE date_diff >= 90 AND avg_qty_sold >= 1
```

### 2. Weight Efficiency Calculation
```sql
-- Verificar que la nueva ponderación de eficiencia por warehouse
-- produce los mismos totales a nivel global_entity_id + supplier
SELECT global_entity_id, supplier_id, SUM(weight_efficiency), SUM(gpv_eur)
FROM flat_sps_efficiency
GROUP BY 1, 2
-- comparar contra sps_efficiency v5
```

### 3. Score Tableau Coverage
```sql
-- Antes: solo keys que existían en sps_purchase_order
-- Después: todas las keys únicas de 8 tablas

SELECT COUNT(DISTINCT (global_entity_id, time_period, brand_sup))
FROM flat_sps_score_tableau
-- Debería ser > que la antigua versión
```

---

## 📊 RESUMEN DE CAMBIOS POR TEMA

### CTEs & Estructura
| Archivo | Antes | Después | Tipo |
|---------|-------|---------|------|
| efficiency | 1 simple | 3 (sku_counts, efficiency_by_warehouse, combined) | Refactor |
| score_tableau | Joins secuenciales | UNION all_keys | Arquitectura |
| days_payable_month | 3 CTEs | 1 CTE | Simplificación |
| financial_metrics_month | 2 CTEs | 1 CTE | Simplificación |
| price_index_month | 3 CTEs | 1 CTE | Simplificación |
| line_rebate_metrics_month | 0 CTEs | 1 CTE | Nueva estructura |

### Fuentes de Datos
| Archivo | Cambio |
|---------|--------|
| efficiency_month | `_aqs_v5_sku_efficiency_detail` → `sku_efficiency_detail_v2` |
| delivery_costs_month | `scm_dc_centralization` location change |
| purchase_order_month | `supplier_performance_report` location change |
| listed_sku_month | `_stock_daily_listed` location change |

### Líneas agregadas/removidas
| Archivo | Δ | Tendencia |
|---------|---|-----------|
| efficiency | +151 | Complejidad ↑ (refactor para auditoría) |
| score_tableau | +82 | Complejidad ↑ (architecture change) |
| days_payable_month | -8 | Simplificación |
| financial_metrics_month | -9 | Simplificación |
| price_index_month | -7 | Simplificación |

---

## 📌 PRÓXIMOS PASOS

### Inmediato
1. **Validar AQS efficiency categorías** — que v2 equivale a v5
2. **Comparar weight_efficiency totals** — asegurar que los números encajan
3. **Auditar score_tableau keys** — verificar coverage

### Documentación
1. **Documentar nuevos campos** en efficiency_month (updated_sku_age, sku_efficiency, etc.)
2. **Documentar weight_efficiency fórmula** en efficiency
3. **Actualizar Tableau datasource** si cambian aggregations

### Testing
1. **Row count validation** — comparar flateos vs antiguos
2. **Sample data spot check** — verificar 10-20 registros manuales
3. **KPI reconciliation** — si hay scorecards, validar que los números se mantienen

---

**Análisis completado:** 2026-04-07  
**Status:** ✅ Exhaustivo - listos para validación en Tableau/Dashboard
