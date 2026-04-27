# SPS Supplier Scoring Architecture

## Overview

El sistema de Supplier Performance Scoring (SPS) es una tubería multi-etapa que transforma datos transaccionales atomizados en scores comparables, justificables y accionables. El flujo tiene 6 etapas:

```
Gathering Data → Grouping Sets → Union All → Segmentation → Scoring → Master
```

Cada etapa es una tabla persistente en BigQuery que downstream consume.

---

## Etapa 1: Gathering Data

### Responsabilidad
Recolectar y parametrizar datos atomizados de múltiples fuentes en una tabla única consolidada.

### Input
Datos distribuidos:
- **Financieros**: Net Sales, COGS, Margins, Discounts, Funding
- **Operacionales**: Fill Rate, OTD, Orders, Customers
- **Técnicos**: Weight Efficiency, Price Index, SKU inventory, Spoilage, Delivery costs

### Output
**Tabla**: `sps_score_tableau` (87 columnas + 15 "ingredients")

### Lógica Clave
- **Entity Parametrization**: Mapeo de supplier → global_entity_id usando 19 entidades y N:N mapping
- **Time Normalization**: Conversión a time_period (YYYYMM) y time_granularity (Monthly/Quarterly/etc)
- **Currency Standardization**: Conversión a moneda local + EUR para comparabilidad
- **Ingredient Extraction**: Cálculo de componentes atómicos que otros procesos usarán:
  - `weight_efficiency`, `price_index_numerator`, `on_time_orders`, `total_received_qty`, etc.
- **Last Year Projection**: Cálculo de Net_Sales_lc_Last_Year para YoY comparisons
- **Nullability Management**: SAFE_DIVIDE y NULLIF en lugares estratégicos

### Ejemplo de Lógica
```sql
-- Parametrización: supplier → entity → time_period
SELECT 
  global_entity_id,      -- Resultado de entity mapping
  time_period,           -- YYYYMM
  time_granularity,      -- 'Monthly'
  Net_Sales_lc,          -- Suma de transacciones
  Net_Sales_eur,         -- Conversión a EUR
  weight_efficiency,     -- Ingrediente: SUM(orders.weight * efficiency_score)
  ...
```

### Validaciones Críticas
- Sin duplicados en (global_entity_id, time_period, supplier_level, division_type, entity_key, brand_sup)
- Todos los ingredientes existentes o NULL (nunca faltantes)
- YoY data consistente con periodos anteriores

---

## Etapa 2: Grouping Sets

### Responsabilidad
Agregar datos desde nivel atómico a nivel de supplier, manteniendo la capacidad de desagregar.

### Input
`sps_score_tableau` (87 columnas con ingredientes crudos)

### Output
Múltiples tablas con GROUP BY en diferentes dimensiones:
- `flat_sps_financial_metrics` — Financieros por supplier
- `flat_sps_efficiency` — Weight efficiency agregado
- `flat_sps_purchase_order` — Fill rate, OTD agregados
- `flat_sps_price_index` — Índice de precios
- `flat_sps_delivery_costs` — Costos de entrega
- etc.

### Lógica Clave
- **Aggregation Function Selection**:
  - `SUM()` para cantidades (Net_Sales, COGS, margins)
  - `APPROX_QUANTILES()` para distribuciones (percentiles)
  - `MAX()` para valores únicos (listed_skus, market_customers)
  - `AVG()` para ratios ya calculados
  
- **GROUP BY Consistency**: Todas usan el mismo conjunto de dimensiones:
  ```sql
  GROUP BY global_entity_id, time_period, time_granularity,
           division_type, supplier_level, entity_key, brand_sup
  ```

- **Ingredient Persistence**: Los cálculos de ingredientes no se pierden — se mantienen como SUM agregados para poder reconstituir ratios en etapa de Master

### Ejemplo
```sql
-- flat_sps_efficiency.sql
SELECT 
  global_entity_id, time_period, ...,
  SUM(weight_efficiency) AS weight_efficiency,  -- Ingrediente agregado
  SUM(gpv_eur) AS gpv_eur,                      -- Denominador
  ROUND(SAFE_DIVIDE(SUM(weight_efficiency), NULLIF(SUM(gpv_eur), 0)), 4)
    AS ratio_efficiency
FROM sps_score_tableau
GROUP BY global_entity_id, time_period, ...
```

### Validaciones Críticas
- Suma de partes = total (validar SUM(efficiency por supplier) = SUM(efficiency en score_tableau))
- No hay ingredientes NULL en el GROUP BY final
- Ratios correctamente derivados de ingredientes

---

## Etapa 3: Union All

### Responsabilidad
Consolidar las múltiples tablas agrupadas en una única tabla de referencia que contiene todas las dimensiones y ratios calculados.

### Input
Output de Etapa 2 (múltiples flat_sps_* tablas)

### Output
**Tabla**: `sps_score_tableau` (versión final publicada)

O bien, una tabla de "reference" que une todos los ratios:
```
global_entity_id | time_period | ratio_otd | ratio_efficiency | ratio_price_index | ...
```

### Lógica Clave
- **Join Strategy**: LEFT JOIN cada tabla de grouping sets por (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup)
- **Null Handling**: COALESCE ratios para caso en que una tabla no tenga datos (e.g., supplier sin POs → ratio_otd = NULL)
- **Deduplication**: Validar que no haya duplicados después del UNION ALL

### Conceptual
```sql
SELECT
  base.global_entity_id,
  base.time_period,
  ...
  financial.ratio_yoy,
  efficiency.ratio_efficiency,
  purchase_order.ratio_otd,
  purchase_order.ratio_fill_rate,
  price_index.ratio_price_index,
  ...
FROM base
LEFT JOIN flat_sps_financial_metrics financial USING (...)
LEFT JOIN flat_sps_efficiency efficiency USING (...)
LEFT JOIN flat_sps_purchase_order po USING (...)
LEFT JOIN flat_sps_price_index pi USING (...)
```

### Validaciones Críticas
- Cada supplier aparece exactamente una vez (no duplicados post-UNION)
- Ratios derivados correctamente del nivel de granularidad correcto
- NULL ratios tienen explicación documentada (e.g., "no hay POs" → otd = NULL)

---

## Etapa 4: Segmentation

### Responsabilidad
Clasificar suppliers en segmentos (Stars, Cows, Dogs, Questions) basándose en tamaño (Net_Sales), productividad y penetración de clientes.

### Input
`sps_score_tableau` (ratios + ingredientes)

### Output
**Tabla**: `sps_supplier_segmentation`

Columnas clave:
- `segment_lc` — Segmento BCG (Stars, Cash Cows, Problem Children, Dogs)
- `importance_score_lc` — Score tamaño/relevancia
- `productivity_score_lc` — Score eficiencia operacional
- `abv_score_lc` — Average Basket Value score
- `frequency_score` — Frecuencia de órdenes score
- `customer_penetration_score` — % de customers alcanzados

### Lógica Clave

**Matriz BCG adaptada para SPS:**
```
            High Productivity
                  ▲
                  │    STARS          COWS
    High Size ────┼──────────────────────
                  │    QUESTIONS      DOGS
                  │
            Low Productivity
```

- **Tamaño (Size)**: PERCENTILE_RANK(Net_Sales_lc) dentro de la entidad
- **Productividad (Productivity)**: PERCENTILE_RANK(ratio_efficiency + ratio_gbd + ratio_back_margin)
- **Customer Metrics**: 
  - `frequency_score` = PERCENTILE_RANK(ratio_frequency)
  - `customer_penetration_score` = PERCENTILE_RANK(ratio_customer_penetration)
  - `abv_score_lc` = PERCENTILE_RANK(ratio_abv)

### Ejemplo de Lógica
```sql
WITH supplier_metrics AS (
  SELECT
    global_entity_id, time_period, entity_key, brand_sup,
    Net_Sales_lc,
    ratio_efficiency,
    ratio_frequency,
    ratio_customer_penetration,
    -- Percentiles dentro de la entidad
    PERCENT_RANK() OVER (
      PARTITION BY global_entity_id, time_period 
      ORDER BY Net_Sales_lc
    ) * 100 AS percentile_size,
    PERCENT_RANK() OVER (
      PARTITION BY global_entity_id, time_period 
      ORDER BY ratio_efficiency
    ) * 100 AS percentile_productivity
  FROM sps_score_tableau
)
SELECT
  *,
  CASE 
    WHEN percentile_size >= 50 AND percentile_productivity >= 50 THEN 'Stars'
    WHEN percentile_size >= 50 AND percentile_productivity < 50 THEN 'Cows'
    WHEN percentile_size < 50 AND percentile_productivity >= 50 THEN 'Questions'
    ELSE 'Dogs'
  END AS segment_lc
FROM supplier_metrics
```

### Validaciones Críticas
- Cada supplier tiene un segmento (nunca NULL)
- Distribución de segmentos es razonable (~25% en cada cuadrante, con variación permitida)
- Scores de customer metrics están normalizados 0-100

---

## Etapa 5: Scoring

### Responsabilidad
Calcular score numérico de cada supplier contra thresholds dinámicos calibrados a la realidad de cada entidad/periodo.

### Input
- `sps_supplier_scoring`: Ratios y scoring params
- `sps_market_yoy`: YoY market benchmark
- `sps_scoring_params`: Thresholds dinámicos

### Output
**Tabla**: `sps_supplier_scoring`

Columnas de scores:
- `score_fill_rate` (0-60)
- `score_otd` (0-40)
- `score_yoy` (0-10)
- `score_efficiency` (0-30)
- `score_gbd` (0-20)
- `score_back_margin` (0-25)
- `score_front_margin` (0-15)
- `operations_score` = score_fill_rate + score_otd (0-100)
- `commercial_score` = score_yoy + score_efficiency + score_gbd + score_back_margin + score_front_margin (0-95)
- `total_score` = (operations_score + commercial_score) / 2 (0-97.5)

### Lógica Clave

**Las 3 capas de thresholds:**

1. **Market Benchmarking** (`sps_market_yoy`):
   - Calcula YoY del mercado agregado por entidad
   - Genera threshold: `yoy_max = LEAST(GREATEST(market_yoy * 1.2, 0.20), 0.70)`
   - Uso: score_yoy escala contra market_yoy, no contra supplier absoluto

2. **Dynamic Parameterization** (`sps_scoring_params`):
   ```
   Back Margin:
   - bm_starting = p25 de suppliers con rebate
   - bm_ending = blend de (IQR_mean * 1.5 + p75) / 2, weighted avg, capped a 70%
   
   Front Margin:
   - fm_starting = MAX(0.12, p25)  ← Floor de 12%
   - fm_ending = blend de (IQR_mean * 1.25 + p75) / 2, weighted avg, capped a 70%
   
   GBD:
   - gbd_target = hardcoded por entidad (FP_SG=13%, TB_AE=8%, etc.)
   - gbd_lower = target * 0.5
   - gbd_upper = target * 2.0
   ```

3. **Individual Scoring Rules**:

   **Fill Rate (60 pts)**:
   ```
   score = MIN(ratio_fill_rate, 1.0) * 60
   ```

   **OTD (40 pts)**:
   ```
   score = MIN(ratio_otd, 1.0) * 40
   ```

   **YoY (10 pts)** — Market-relative:
   ```
   IF ratio_yoy <= 0: score = 0
   IF ratio_yoy >= yoy_max: score = 10
   ELSE: score = (ratio_yoy / yoy_max) * 10
   ```

   **Efficiency (30 pts)**:
   ```
   IF ratio_efficiency < 0.40: score = 0
   IF ratio_efficiency >= 1.0: score = 30
   ELSE: score = ((ratio_efficiency - 0.40) / 0.60) * 30
   ```

   **GBD (20 pts)** — Asymmetric bell curve:
   ```
   IF ratio_gbd < gbd_lower: score = 0
   IF ratio_gbd <= gbd_target: 
      score = ((ratio_gbd - gbd_lower) / (gbd_target - gbd_lower)) * 20
   IF ratio_gbd <= gbd_upper:
      score = (1.0 - ((ratio_gbd - gbd_target) / (gbd_upper - gbd_target)) * 0.5) * 20
   ELSE: score = 0
   ```
   (Penaliza GBD que es muy bajo O muy alto)

   **Back Margin (25 pts)**:
   ```
   IF has_rebate = 0: score = 0
   IF ratio_back_margin < bm_starting: score = 0
   IF ratio_back_margin >= bm_ending: score = 25
   ELSE: score = ((ratio_back_margin - bm_starting) / (bm_ending - bm_starting)) * 25
   ```

   **Front Margin (15 pts)**:
   ```
   IF ratio_front_margin <= 0: score = 0
   IF ratio_front_margin < fm_starting: score = 0
   IF ratio_front_margin >= fm_ending: score = 15
   ELSE: score = ((ratio_front_margin - fm_starting) / (fm_ending - fm_starting)) * 15
   ```

### Explainability
Cada score está acompañado de sus thresholds exactos:
```
ratio_yoy | threshold_yoy_max | score_yoy
0.10      | 0.35              | 2.86  ← (0.10/0.35)*10
```

Tableau puede usar estos threshold_* para mostrar "dónde estás vs dónde deberías estar".

### Validaciones Críticas
- Score total entre 0-97.5 (nunca > 100, nunca negativo)
- operations_score (0-100) y commercial_score (0-95) consistentes
- Todos los thresholds derivados de datos reales (percentiles, weighted averages)
- Score aumenta monótonamente con ratio (no hay inversiones lógicas)

---

## Etapa 6: Master

### Responsabilidad
Consolidar en una única tabla desnormalizada:
- Todos los ingredientes crudos
- Todos los ratios operacionales
- Todos los scores
- Todos los thresholds
- Segmentación
- Numeradores de weighted scores para agregación correcta en Tableau

### Input
- `sps_supplier_scoring` (scores + thresholds)
- `sps_supplier_segmentation` (segmentos)
- Base de ingredientes crudos (financieros, operacionales, técnicos)

### Output
**Tabla**: `sps_supplier_master` (87 columnas iniciales + 12 ratios + 10 scores + 8 thresholds + 6 segmentación + 10 weighted score numerators)

**Total: ~140 columnas, CLUSTERED BY (global_entity_id, time_period)**

### Lógica Clave

**Secciones de la tabla:**

1. **Identidad** (7 cols):
   - global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup

2. **Ingredientes Crudos** (31 cols):
   - Net_Sales_lc, Net_Sales_eur, COGS_lc, margins, discounts, funding
   - weight_efficiency, gpv_eur, price_index_numerator, price_index_weight
   - on_time_orders, total_non_cancelled_po_orders, received_qty, demanded_qty, cancelled_po_orders
   - listed_skus, spoilage, retail_revenue, stock_value, days_in_month, delivery_cost

3. **Ratios Operacionales** (12 cols):
   ```
   ratio_otd, ratio_fill_rate, ratio_efficiency, ratio_price_index, 
   ratio_yoy, ratio_back_margin, ratio_front_margin, ratio_gbd,
   ratio_promo_contribution, ratio_abv, ratio_frequency, 
   ratio_customer_penetration, ratio_spoilage, ratio_doh, ratio_delivery_cost,
   ratio_net_profit_margin
   ```

4. **Scores Individuales** (7 cols):
   - score_fill_rate, score_otd, score_yoy, score_efficiency, score_gbd, score_back_margin, score_front_margin

5. **Scores Agregados** (3 cols):
   - operations_score (fill_rate + otd)
   - commercial_score (yoy + efficiency + gbd + back_margin + front_margin)
   - total_score ((operations + commercial) / 2)

6. **Thresholds** (8 cols):
   ```
   threshold_yoy_max, threshold_bm_start, threshold_bm_end,
   threshold_fm_start, threshold_fm_end, threshold_gbd_target, 
   threshold_gbd_lower, threshold_gbd_upper
   ```

7. **Segmentación** (6 cols):
   ```
   segment_lc, importance_score_lc, productivity_score_lc,
   abv_score_lc, frequency_score, customer_penetration_score
   ```

8. **Weighted Score Numerators** (10 cols):
   ```
   wscore_num_fill_rate = score_fill_rate * Net_Sales_lc
   wscore_num_otd = score_otd * Net_Sales_lc
   wscore_num_yoy = score_yoy * Net_Sales_lc
   wscore_num_efficiency = score_efficiency * Net_Sales_lc
   wscore_num_gbd = score_gbd * Net_Sales_lc
   wscore_num_back_margin = score_back_margin * Net_Sales_lc
   wscore_num_front_margin = score_front_margin * Net_Sales_lc
   wscore_num_operations = operations_score * Net_Sales_lc
   wscore_num_commercial = commercial_score * Net_Sales_lc
   wscore_num_total = total_score * Net_Sales_lc
   ```

9. **Weighted Score Denominator** (1 col):
   ```
   wscore_denom = Net_Sales_lc
   ```

### El Genio de los Weighted Scores

**Problema sin solución**: ¿Cómo hago un promedio ponderado de scores cuando los data viven en diferentes niveles de granularidad (supplier vs division vs entity)?

**Solución**: Almacenar numeradores separados.

```sql
-- En Tableau, para calcular weighted avg de score_fill_rate por entity:
SELECT
  global_entity_id,
  SUM(wscore_num_fill_rate) / SUM(wscore_denom) AS weighted_avg_fill_rate
FROM sps_supplier_master
WHERE global_entity_id = 'FP_SG' AND time_period = '202604'
GROUP BY global_entity_id
```

Esto **escala correctamente** porque:
- SUM(wscore_num_fill_rate) = SUM(score_fill_rate * Net_Sales_lc)
- SUM(wscore_denom) = SUM(Net_Sales_lc)
- Resultado = SUM(score_fill_rate * Net_Sales_lc) / SUM(Net_Sales_lc) ← El promedio ponderado correcto

### Joins Clave
```sql
FROM base b
LEFT JOIN sps_supplier_scoring sc 
  ON (global_entity_id, time_period, time_granularity, 
      division_type, supplier_level, entity_key, brand_sup)
LEFT JOIN sps_supplier_segmentation seg 
  ON (global_entity_id, time_period, time_granularity, 
      division_type, supplier_level, entity_key, brand_sup)
```

### Validaciones Críticas
- Ningún duplicado (verificar COUNT(*) = COUNT(DISTINCT <identity keys>))
- wscore_numerators son consistentes con scores (wscore_num_fill_rate / wscore_denom ≈ score_fill_rate)
- Todas las columnas de thresholds pobladas (nunca NULL)
- Cluster efectivo (verificar que queries filtren por global_entity_id y time_period)

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    GATHERING DATA                           │
│  Transactional data + entity mapping + time normalization   │
│              → sps_score_tableau (87 cols)                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    GROUPING SETS                            │
│  GROUP BY (entity, period, supplier) + SUM aggregates       │
│  → flat_sps_financial_metrics, flat_sps_efficiency,         │
│    flat_sps_purchase_order, flat_sps_price_index, etc.      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    UNION ALL                                │
│  LEFT JOIN all grouping sets into reference table           │
│          → sps_score_tableau v2 (all ratios)               │
└────────────────────────┬────────────────────────────────────┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
    ┌──────────┐  ┌────────────────┐  ┌──────────────────┐
    │Segment-  │  │  Market YoY    │  │Scoring Params    │
    │ation     │  │  Benchmark     │  │(Dynamic thresh)  │
    │          │  │                │  │                  │
    │sps_      │  │sps_market_yoy  │  │sps_scoring_params│
    │supplier_ │  │                │  │                  │
    │segment-  │  │ - market_yoy_lc│  │ - bm_starting/   │
    │ation     │  │   by entity    │  │   bm_ending      │
    │          │  │                │  │ - fm_starting/   │
    └──────────┘  └────────────────┘  │   fm_ending      │
          │              │             │ - gbd_target/   │
          │              │             │   lower/upper    │
          │              └─────────────┤                  │
          │                            │                  │
          ▼                            ▼                  ▼
    ┌─────────────────────────────────────────────────────┐
    │            SCORING                                   │
    │  Calcula 7 score_* individuales contra thresholds    │
    │  + operations_score + commercial_score + total_score │
    │                                                       │
    │        → sps_supplier_scoring (65 cols)              │
    └────────────┬───────────────────────────────────────┘
                 │
                 ▼
    ┌──────────────────────────────────┐
    │      MASTER (Denormalization)    │
    │                                  │
    │ - Base ingredients (31 cols)     │
    │ - Ratios (12 cols)               │
    │ - Scores (10 cols)               │
    │ - Thresholds (8 cols)            │
    │ - Segmentation (6 cols)          │
    │ - Weighted score numerators (10) │
    │                                  │
    │ → sps_supplier_master (140 cols) │
    │   CLUSTERED BY (entity, period)  │
    └──────────────────────────────────┘
                 │
                 ▼
        ┌─────────────────┐
        │  Tableau Dashboards
        │  (joins disabled) │
        └─────────────────┘
```

---

## Performance & Indexing

- **sps_supplier_master** está CLUSTERED BY (global_entity_id, time_period) → queries filtrando por entidad y período son O(1)
- **Partitioning** recomendado: RANGE(time_period) por semestre o año
- **Materialize weighte scores**: Los numeradores y denominador están **almacenados**, no calculados on-the-fly en Tableau

---

## Governance & Validation

### SLA
- Todos los scripts corre diariamente en horario L-V 6am UTC
- SLA de actualización: 8am UTC (max latency 2h desde ingesta)

### Validaciones Automáticas
- Uniqueness por (global_entity_id, time_period, division_type, supplier_level, entity_key, brand_sup)
- No NULL en score_* columns
- Distribución de scores 0-97.5 sin outliers > 100
- Weighted average de scores ≈ promedio aritmético (control de lógica)

### Manual Spot Checks
- Comparar top 5 suppliers por entidad/periodo con expectativas operacionales
- Validar que Market YoY tiene sentido (e.g., market no crece 200% en un mes)
- Chequear que gbd scores reflejan política de descuentos por entidad

---

## Conclusión

Este arquitectura resuelve:
1. ✅ **Comparabilidad**: Thresholds dinámicos responden a realidad local, no hardcoding
2. ✅ **Justificabilidad**: Cada score está acompañado de los thresholds que lo generaron
3. ✅ **Escalabilidad**: 19 entidades, 9 prefixes, N:N mappings, todo parametrizado
4. ✅ **Agregación Correcta**: Weighted scores se calculan correctamente en Tableau sin duplicación
5. ✅ **Trazabilidad**: Desde ingrediente atómico hasta score final es lineal y auditable

**Para Igor**: Tienes acceso a sps_supplier_master. Todo lo que necesitas ya está calculado.
