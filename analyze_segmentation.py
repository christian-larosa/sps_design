from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
import numpy as np

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

df = client.query(query).to_dataframe()

# ── Convertir Decimal a float ────────────────────────────────────────────────
for col in df.columns:
    if df[col].dtype == 'object':
        try:
            df[col] = df[col].astype(float)
        except:
            pass

# ── Orden y colores de segmentos ─────────────────────────────────────────────
segment_order  = ['Key Accounts', 'Standard', 'Niche', 'Long Tail']
segment_colors = {
    'Key Accounts': '#1a5276',
    'Standard':     '#2e86c1',
    'Niche':        '#85c1e9',
    'Long Tail':    '#d5e8f0'
}
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

# ── Plot: boxplots por KPI por segmento ──────────────────────────────────────
fig, axes = plt.subplots(2, 3, figsize=(18, 10))
fig.suptitle('KPI Distribution by Segment — PY_PE division | March 2026',
             fontsize=14, fontweight='bold', y=1.01)

for ax, (col, label) in zip(axes.flatten(), kpis.items()):
    data_by_segment = [
        df[df['segment_lc'] == seg][col].dropna().values
        for seg in segment_order
    ]
    bp = ax.boxplot(
        data_by_segment,
        labels=segment_order,
        patch_artist=True,
        medianprops=dict(color='white', linewidth=2),
        whiskerprops=dict(linewidth=1.2),
        capprops=dict(linewidth=1.2),
        flierprops=dict(marker='o', markersize=3, alpha=0.4)
    )
    for patch, seg in zip(bp['boxes'], segment_order):
        patch.set_facecolor(segment_colors[seg])
        patch.set_alpha(0.85)

    ax.set_title(label, fontsize=11, fontweight='bold')
    ax.set_xlabel('')
    ax.tick_params(axis='x', labelsize=9)
    ax.tick_params(axis='y', labelsize=9)
    ax.grid(axis='y', alpha=0.3)

    # Anotar mediana encima de cada box
    for i, seg in enumerate(segment_order):
        med = df[df['segment_lc'] == seg][col].median()
        if not np.isnan(med):
            ax.text(i + 1, ax.get_ylim()[1] * 0.97,
                    f'{med:.1f}', ha='center', va='top',
                    fontsize=8, color='#333333')

plt.tight_layout()
plt.savefig('segment_kpi_distribution.png', dpi=150, bbox_inches='tight')
plt.show()
print("Plot guardado: segment_kpi_distribution.png")

# ── Summary stats por segmento ───────────────────────────────────────────────
summary = df.groupby('segment_lc', observed=True)[list(kpis.keys())].agg([
    'median', 'mean', 'std',
    lambda x: x.quantile(0.25),
    lambda x: x.quantile(0.75)
])
summary.columns = ['_'.join(c) for c in summary.columns]
print("\n── Summary Stats por Segmento ──────────────────────────────────────")
print(summary.to_string())

# ── Conteo por segmento ──────────────────────────────────────────────────────
print("\n── Conteo de suppliers por segmento ────────────────────────────────")
print(df['segment_lc'].value_counts().sort_index())
