CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.sps_scoring_params`
AS
WITH

bm_base AS (
  SELECT global_entity_id, time_period, entity_key,
    SAFE_DIVIDE(SUM(back_margin_amt_lc), NULLIF(SUM(Net_Sales_lc),0)) AS bm_ratio,
    MAX(CASE WHEN back_margin_amt_lc > 0 THEN 1 ELSE 0 END)           AS has_rebate,
    SUM(back_margin_amt_lc)  AS bm_lc,
    SUM(Net_Sales_lc)        AS net_sales_lc
  FROM `dh-darkstores-live.csm_automated_tables.sps_score_tableau`
  WHERE supplier_level = 'supplier' AND time_granularity = 'Monthly'
    AND division_type = 'division' AND Net_Sales_eur > 1000
  GROUP BY global_entity_id, time_period, entity_key, brand_sup
),
bm_pcts AS (
  SELECT global_entity_id, time_period,
    APPROX_QUANTILES(CASE WHEN has_rebate=1 THEN bm_ratio END,100)[OFFSET(25)] AS bm_p25,
    APPROX_QUANTILES(CASE WHEN has_rebate=1 THEN bm_ratio END,100)[OFFSET(75)] AS bm_p75,
    SAFE_DIVIDE(SUM(CASE WHEN has_rebate=1 THEN bm_lc END),
      NULLIF(SUM(CASE WHEN has_rebate=1 THEN net_sales_lc END),0))              AS bm_weighted
  FROM bm_base GROUP BY global_entity_id, time_period
),
bm_iqr AS (
  SELECT b.global_entity_id, b.time_period,
    AVG(CASE WHEN b.has_rebate=1
      AND b.bm_ratio BETWEEN p.bm_p25 AND p.bm_p75
      THEN b.bm_ratio END) AS bm_iqr_mean
  FROM bm_base b JOIN bm_pcts p USING (global_entity_id, time_period)
  GROUP BY b.global_entity_id, b.time_period
),

fm_base AS (
  SELECT global_entity_id, time_period, entity_key,
    SAFE_DIVIDE(
      SUM(front_margin_amt_lc) + SUM(total_supplier_funding_lc),
      NULLIF(SUM(Net_Sales_lc),0)
    ) AS fm_ratio,
    SUM(front_margin_amt_lc) + SUM(total_supplier_funding_lc) AS fm_lc,
    SUM(Net_Sales_lc) AS net_sales_lc
  FROM `dh-darkstores-live.csm_automated_tables.sps_score_tableau`
  WHERE supplier_level = 'supplier' AND time_granularity = 'Monthly'
    AND division_type = 'division' AND Net_Sales_eur > 1000 AND COGS_lc > 0
  GROUP BY global_entity_id, time_period, entity_key, brand_sup
),
fm_pcts AS (
  SELECT global_entity_id, time_period,
    GREATEST(0.12,
      APPROX_QUANTILES(CASE WHEN fm_ratio > 0 THEN fm_ratio END,100)[OFFSET(25)]
    ) AS fm_starting,
    APPROX_QUANTILES(CASE WHEN fm_ratio > 0 THEN fm_ratio END,100)[OFFSET(75)] AS fm_p75,
    SAFE_DIVIDE(SUM(CASE WHEN fm_ratio > 0 THEN fm_lc END),
      NULLIF(SUM(CASE WHEN fm_ratio > 0 THEN net_sales_lc END),0))              AS fm_weighted
  FROM fm_base GROUP BY global_entity_id, time_period
),
fm_iqr AS (
  SELECT b.global_entity_id, b.time_period,
    AVG(CASE WHEN b.fm_ratio > 0
      AND b.fm_ratio BETWEEN p.fm_starting AND p.fm_p75
      THEN b.fm_ratio END) AS fm_iqr_mean
  FROM fm_base b JOIN fm_pcts p USING (global_entity_id, time_period)
  GROUP BY b.global_entity_id, b.time_period
),

gbd_targets AS (
  SELECT global_entity_id, gbd_target FROM UNNEST([
    STRUCT('FP_SG' AS global_entity_id, 0.130 AS gbd_target),
    STRUCT('FP_PH', 0.040), STRUCT('FP_HK', 0.200),
    STRUCT('NP_HU', 0.055), STRUCT('YS_TR', 0.200),
    STRUCT('TB_BH', 0.080), STRUCT('TB_JO', 0.050),
    STRUCT('TB_AE', 0.080), STRUCT('TB_OM', 0.070),
    STRUCT('TB_KW', 0.050), STRUCT('HF_EG', 0.050),
    STRUCT('TB_QA', 0.076), STRUCT('HS_SA', 0.075),
    STRUCT('PY_AR', 0.150), STRUCT('PY_PE', 0.080),
    STRUCT('PY_CL', 0.090), STRUCT('GV_ES', 0.034),
    STRUCT('GV_IT', 0.040), STRUCT('GV_UA', 0.050)
  ])
)

SELECT
  p.global_entity_id,
  p.time_period,
  -- Back margin
  p.bm_p25                                                            AS bm_starting,
  LEAST(GREATEST(
    (i.bm_iqr_mean * 1.5 + p.bm_p75) / 2,
    p.bm_weighted
  ), 0.70)                                                            AS bm_ending,
  -- Front margin
  p2.fm_starting,
  LEAST(GREATEST(
    (i2.fm_iqr_mean * 1.25 + p2.fm_p75) / 2,
    p2.fm_weighted,
    p2.fm_starting + 0.08
  ), 0.70)                                                            AS fm_ending,
  -- GBD
  COALESCE(g.gbd_target, 0.05)                                        AS gbd_target,
  COALESCE(g.gbd_target, 0.05) * 0.5                                  AS gbd_lower,
  COALESCE(g.gbd_target, 0.05) * 2.0                                  AS gbd_upper
FROM bm_pcts p
JOIN bm_iqr  i  USING (global_entity_id, time_period)
JOIN fm_pcts p2 USING (global_entity_id, time_period)
JOIN fm_iqr  i2 USING (global_entity_id, time_period)
LEFT JOIN gbd_targets g ON p.global_entity_id = g.global_entity_id
