from google.cloud import bigquery
import pandas as pd
import numpy as np
from decimal import Decimal

client = bigquery.Client(project='dh-darkstores-live')

query = """
SELECT
  time_period,
  division_type,
  entity_key,
  segment_lc,
  net_profit_lc,
  abv_lc_order,
  frequency,
  customer_penetration,
  importance_score_lc,
  abv_score_lc,
  frequency_score,
  customer_penetration_score,
  productivity_score_lc,
  gpv_flag
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_segmentation`
WHERE time_period   = '2026-03-01'
  AND division_type = 'division'
  AND gpv_flag      = 'OK'
"""

print("Fetching data from BigQuery...")
df = client.query(query).to_dataframe()

# ── Convertir Decimal a float ────────────────────────────────────────────────
for col in df.columns:
    if df[col].dtype == 'object':
        try:
            df[col] = pd.to_numeric(df[col])
        except:
            pass

# ── Orden de segmentos ────────────────────────────────────────────────────────
segment_order  = ['Key Accounts', 'Standard', 'Niche', 'Long Tail']
df['segment_lc'] = pd.Categorical(df['segment_lc'], categories=segment_order, ordered=True)
df = df.sort_values('segment_lc')

# ── KPIs a analizar ──────────────────────────────────────────────────────────
kpis = {
    'net_profit_lc':          'Net Profit LC',
    'abv_lc_order':           'ABV LC (basket value)',
    'frequency':              'Frequency (orders/customer)',
    'customer_penetration':   'Customer Penetration %',
    'importance_score_lc':    'Importance Score (0-100)',
    'productivity_score_lc':  'Productivity Score (0-100)',
}

# ── Conteo por segmento ──────────────────────────────────────────────────────
print("\n" + "="*70)
print("── Conteo de suppliers por segmento ────────────────────────────────")
print("="*70)
segment_counts = df['segment_lc'].value_counts().sort_index()
for seg in segment_order:
    count = segment_counts.get(seg, 0)
    pct = 100 * count / len(df) if len(df) > 0 else 0
    print(f"{seg:20} {count:4d}  ({pct:5.1f}%)")
print(f"{'TOTAL':20} {len(df):4d}  (100.0%)")

# ── Summary stats por segmento ───────────────────────────────────────────────
print("\n" + "="*70)
print("── Summary Statistics by Segment ──────────────────────────────────")
print("="*70)

for seg in segment_order:
    seg_data = df[df['segment_lc'] == seg]
    if len(seg_data) == 0:
        continue

    print(f"\n{seg} (n={len(seg_data)})")
    print("-" * 70)
    for col, label in kpis.items():
        med = seg_data[col].median()
        mean = seg_data[col].mean()
        std = seg_data[col].std()
        q25 = seg_data[col].quantile(0.25)
        q75 = seg_data[col].quantile(0.75)

        print(f"  {label:35} median={med:8.2f}  mean={mean:8.2f}  std={std:7.2f}  [Q1={q25:6.2f}, Q3={q75:6.2f}]")

print("\n" + "="*70)
print("Analysis complete!")
