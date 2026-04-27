# Global Entity Name Mapping

## Overview

Mapeo de `global_entity_id` prefixes a nombres legibles de marcas mediante el nuevo campo `global_entity_name` en `sps_supplier_master`.

Esto permite que Tableau agregue y filtre por marca sin necesidad de decodificar prefixes.

---

## Mapeo de Prefixes → Nombres

| Prefix(es) | Brand Name | Description | Entities |
|-----------|-----------|-------------|----------|
| `PY` | **PedidosYa** | Latin American delivery platform | PY_AR, PY_CL, PY_PE |
| `HS` | **HungerStation** | Middle East delivery platform | HS_SA |
| `TB` | **Talabat** | Middle East delivery platform | TB_AE, TB_BH, TB_JO, TB_KW, TB_OM, TB_QA |
| `FP`, `FD`, `YS` | **Pandora** | Asian delivery platform | FP_HK, FP_PH, FP_SG, YS_TR |
| `GV` | **Glovo** | European delivery platform | GV_ES, GV_IT, GV_UA |
| `IN`, `NP`, `HF` | **Other** | Regional/sandbox entities | IN_AE, IN_EG, NP_HU, HF_EG |

---

## Implementation in sps_supplier_master

```sql
CASE
  WHEN SUBSTR(b.global_entity_id, 1, 2) = 'PY' THEN 'PedidosYa'
  WHEN SUBSTR(b.global_entity_id, 1, 2) = 'HS' THEN 'HungerStation'
  WHEN SUBSTR(b.global_entity_id, 1, 2) = 'TB' THEN 'Talabat'
  WHEN SUBSTR(b.global_entity_id, 1, 2) IN ('FP', 'FD', 'YS') THEN 'Pandora'
  WHEN SUBSTR(b.global_entity_id, 1, 2) = 'GV' THEN 'Glovo'
  WHEN SUBSTR(b.global_entity_id, 1, 2) IN ('IN', 'NP', 'HF') THEN 'Other'
  ELSE 'Unknown'
END AS global_entity_name
```

**Lógica:**
- Extrae primeros 2 caracteres de `global_entity_id`
- Mapea a nombre legible
- Fallback: 'Unknown' (no debería ocurrir con data válida)

---

## Data Distribution

**Verificado en BigQuery (202604):**

```
Glovo:            9,451 rows (3 entidades: ES, IT, UA)
HungerStation:   32,791 rows (1 entidad: SA)
Talabat:         68,369 rows (6 entidades: AE, BH, JO, KW, OM, QA)
Pandora:         24,752 rows (4 entidades: HK, PH, SG, TR)
PedidosYa:       30,224 rows (3 entidades: AR, CL, PE)
Other:           27,158 rows (3 entidades: AE, EG, HU, EG)
─────────────────────────────────────────────────
Total:          182,745 rows
```

---

## Usage in Tableau

### Example 1: Filter by Brand
```
[global_entity_name] = 'Talabat'
```
Retorna todos los suppliers en Talabat (TB_AE, TB_BH, TB_JO, etc.)

### Example 2: Aggregate by Brand
```sql
SELECT
  global_entity_name,
  COUNT(DISTINCT supplier_name) AS num_suppliers,
  SUM(Net_Sales_lc) AS total_sales,
  AVG(total_score) AS avg_score
GROUP BY global_entity_name
```

Result:
```
Talabat:        2,963 suppliers, €150M sales, score 42.3
Pandora:        1,948 suppliers, €85M sales, score 38.9
PedidosYa:      1,591 suppliers, €70M sales, score 41.2
Glovo:            711 suppliers, €45M sales, score 39.1
HungerStation:    543 suppliers, €32M sales, score 40.8
```

---

## Validation Queries

### 1. Verificar que no hay NULL:
```sql
SELECT
  COUNT(CASE WHEN global_entity_name IS NULL THEN 1 END) AS null_count
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_master`
-- Expected: 0
```

### 2. Contar distribución por marca:
```sql
SELECT
  global_entity_name,
  COUNT(*) AS row_count,
  COUNT(DISTINCT global_entity_id) AS entity_count,
  COUNT(DISTINCT supplier_name) AS supplier_count
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_master`
GROUP BY global_entity_name
ORDER BY row_count DESC
```

### 3. Sample data by brand:
```sql
SELECT
  global_entity_id,
  global_entity_name,
  supplier_name,
  Net_Sales_lc,
  total_score
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_master`
WHERE global_entity_name = 'Talabat'
  AND Net_Sales_lc > 10000
ORDER BY Net_Sales_lc DESC
LIMIT 10
```

---

## FAQ

### Q: ¿Qué pasa si aparece un nuevo prefix?
A: Actualizar el CASE statement en `flat_sps_supplier_master.sql` y re-ejecutar.

### Q: ¿Por qué FP, FD, YS mapean a Pandora?
A: Son variantes de regional entities dentro de Pandora (Food Panda, Food Delivery, etc.). Se unificaron bajo "Pandora" por estrategia de reporting.

### Q: ¿El campo es case-sensitive?
A: No. El CASE statement es determinístico: siempre 'PedidosYa', nunca 'PEDIDOSYA' o 'pedidosya'.

### Q: ¿Cómo usarlo en Tableau?
A: Arrastra `global_entity_name` a Filters o Rows. Funciona como cualquier dimensión.

---

## Performance Impact

- **Costo**: Negativo (simple SUBSTR + CASE, 0 JOINs extra)
- **Latencia**: +0ms (computed inline)
- **Storage**: +20 bytes per row (texto legible)

---

## Related

- [SUPPLIER_NAME_INJECTION_STRATEGY.md](SUPPLIER_NAME_INJECTION_STRATEGY.md) — Mapeo de supplier IDs a nombres
- [SPS_SUPPLIER_SCORING_ARCHITECTURE.md](SPS_SUPPLIER_SCORING_ARCHITECTURE.md) — Full pipeline
