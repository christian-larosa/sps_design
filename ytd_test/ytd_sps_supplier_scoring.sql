CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.ytd_sps_supplier_scoring`
AS
WITH

ratios AS (
  SELECT
    st.global_entity_id, st.time_period, st.time_granularity,
    st.division_type, st.supplier_level, st.entity_key, st.brand_sup,
    st.fill_rate,
    st.otd,
    SAFE_DIVIDE(
      SUM(st.Net_Sales_eur) - SUM(st.Net_Sales_eur_Last_Year),
      NULLIF(SUM(st.Net_Sales_eur_Last_Year), 0)
    )                                                                   AS yoy_growth,
    SAFE_DIVIDE(SUM(st.weight_efficiency), NULLIF(SUM(st.gpv_eur), 0)) AS efficiency_ratio,
    SAFE_DIVIDE(
      SUM(st.total_discount_lc),
      NULLIF(SUM(st.Net_Sales_lc) + SUM(st.total_discount_lc), 0)
    )                                                                   AS gbd,
    SAFE_DIVIDE(SUM(st.back_margin_amt_lc), NULLIF(SUM(st.Net_Sales_lc), 0))
                                                                        AS back_margin_ratio,
    MAX(CASE WHEN st.back_margin_amt_lc > 0 THEN 1 ELSE 0 END)         AS has_rebate,
    SAFE_DIVIDE(
      SUM(st.front_margin_amt_lc) + SUM(st.total_supplier_funding_lc),
      NULLIF(SUM(st.Net_Sales_lc), 0)
    )                                                                   AS front_margin_ratio
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_score_tableau` st
  WHERE st.supplier_level    = 'supplier'
    AND st.time_granularity  = 'Monthly'
    AND st.division_type     IN ('division', 'principal')
    AND st.Net_Sales_eur     > 1000
  GROUP BY
    st.global_entity_id, st.time_period, st.time_granularity,
    st.division_type, st.supplier_level, st.entity_key, st.brand_sup,
    st.fill_rate, st.otd
),

yoy_thresh AS (
  SELECT global_entity_id, time_period, time_granularity,
    LEAST(GREATEST(market_yoy_lc * 1.2, 0.20), 0.70) AS yoy_max
  FROM `dh-darkstores-live.csm_automated_tables.ytd_sps_market_yoy`
)

SELECT
  r.global_entity_id, r.time_period, r.time_granularity,
  r.division_type, r.supplier_level, r.entity_key, r.brand_sup,

  -- Ratios
  ROUND(r.fill_rate, 4)          AS ratio_fill_rate,
  ROUND(r.otd, 4)                AS ratio_otd,
  ROUND(r.yoy_growth, 4)         AS ratio_yoy,
  ROUND(r.efficiency_ratio, 4)   AS ratio_efficiency,
  ROUND(r.gbd, 4)                AS ratio_gbd,
  ROUND(r.back_margin_ratio, 4)  AS ratio_back_margin,
  ROUND(r.front_margin_ratio, 4) AS ratio_front_margin,

  -- Thresholds (para explainability en Tableau)
  ROUND(y.yoy_max, 4)            AS threshold_yoy_max,
  ROUND(p.bm_starting, 4)        AS threshold_bm_start,
  ROUND(p.bm_ending, 4)          AS threshold_bm_end,
  ROUND(p.fm_starting, 4)        AS threshold_fm_start,
  ROUND(p.fm_ending, 4)          AS threshold_fm_end,
  ROUND(p.gbd_target, 4)         AS threshold_gbd_target,
  ROUND(p.gbd_lower, 4)          AS threshold_gbd_lower,
  ROUND(p.gbd_upper, 4)          AS threshold_gbd_upper,

  -- Scores individuales
  -- Fill Rate 60 pts
  IFNULL(ROUND(LEAST(r.fill_rate, 1.0) * 60, 2), 0)
    AS score_fill_rate,

  -- OTD 40 pts
  IFNULL(ROUND(LEAST(r.otd, 1.0) * 40, 2), 0)
    AS score_otd,

  -- YoY 10 pts
  ROUND(CASE
    WHEN r.yoy_growth IS NULL OR r.yoy_growth <= 0  THEN 0.0
    WHEN r.yoy_growth >= y.yoy_max                  THEN 10.0
    ELSE (r.yoy_growth / y.yoy_max) * 10.0
  END, 2) AS score_yoy,

  -- Efficiency 30 pts
  ROUND(CASE
    WHEN r.efficiency_ratio IS NULL
      OR r.efficiency_ratio < 0.40  THEN 0.0
    WHEN r.efficiency_ratio >= 1.0  THEN 30.0
    ELSE ((r.efficiency_ratio - 0.40) / 0.60) * 30.0
  END, 2) AS score_efficiency,

  -- GBD 20 pts — asymmetric bell curve
  ROUND(CASE
    WHEN r.gbd IS NULL OR r.gbd <= 0         THEN 0.0
    WHEN r.gbd < p.gbd_lower                 THEN 0.0
    WHEN r.gbd <= p.gbd_target               THEN
      ((r.gbd - p.gbd_lower)
        / NULLIF(p.gbd_target - p.gbd_lower, 0)) * 20.0
    WHEN r.gbd <= p.gbd_upper                THEN
      (1.0 - ((r.gbd - p.gbd_target)
        / NULLIF(p.gbd_upper - p.gbd_target, 0)) * 0.5) * 20.0
    ELSE 0.0
  END, 2) AS score_gbd,

  -- Back Margin 25 pts
  ROUND(CASE
    WHEN r.has_rebate = 0
      OR r.back_margin_ratio IS NULL          THEN 0.0
    WHEN r.back_margin_ratio < p.bm_starting  THEN 0.0
    WHEN r.back_margin_ratio >= p.bm_ending   THEN 25.0
    ELSE ((r.back_margin_ratio - p.bm_starting)
      / NULLIF(p.bm_ending - p.bm_starting, 0)) * 25.0
  END, 2) AS score_back_margin,

  -- Front Margin 15 pts
  ROUND(CASE
    WHEN r.front_margin_ratio IS NULL
      OR r.front_margin_ratio <= 0            THEN 0.0
    WHEN r.front_margin_ratio < p.fm_starting THEN 0.0
    WHEN r.front_margin_ratio >= p.fm_ending  THEN 15.0
    ELSE ((r.front_margin_ratio - p.fm_starting)
      / NULLIF(p.fm_ending - p.fm_starting, 0)) * 15.0
  END, 2) AS score_front_margin,

  -- Sub-totales
  IFNULL(ROUND(LEAST(r.fill_rate,1.0)*60,2),0)
    + IFNULL(ROUND(LEAST(r.otd,1.0)*40,2),0)
    AS operations_score,

  ROUND(CASE WHEN r.yoy_growth IS NULL OR r.yoy_growth<=0 THEN 0.0
    WHEN r.yoy_growth>=y.yoy_max THEN 10.0
    ELSE (r.yoy_growth/y.yoy_max)*10.0 END,2)
  + ROUND(CASE WHEN r.efficiency_ratio IS NULL OR r.efficiency_ratio<0.40 THEN 0.0
    WHEN r.efficiency_ratio>=1.0 THEN 30.0
    ELSE ((r.efficiency_ratio-0.40)/0.60)*30.0 END,2)
  + ROUND(CASE WHEN r.gbd IS NULL OR r.gbd<=0 OR r.gbd<p.gbd_lower THEN 0.0
    WHEN r.gbd<=p.gbd_target THEN ((r.gbd-p.gbd_lower)/NULLIF(p.gbd_target-p.gbd_lower,0))*20.0
    WHEN r.gbd<=p.gbd_upper  THEN (1.0-((r.gbd-p.gbd_target)/NULLIF(p.gbd_upper-p.gbd_target,0))*0.5)*20.0
    ELSE 0.0 END,2)
  + ROUND(CASE WHEN r.has_rebate=0 OR r.back_margin_ratio IS NULL OR r.back_margin_ratio<p.bm_starting THEN 0.0
    WHEN r.back_margin_ratio>=p.bm_ending THEN 25.0
    ELSE ((r.back_margin_ratio-p.bm_starting)/NULLIF(p.bm_ending-p.bm_starting,0))*25.0 END,2)
  + ROUND(CASE WHEN r.front_margin_ratio IS NULL OR r.front_margin_ratio<=0 OR r.front_margin_ratio<p.fm_starting THEN 0.0
    WHEN r.front_margin_ratio>=p.fm_ending THEN 15.0
    ELSE ((r.front_margin_ratio-p.fm_starting)/NULLIF(p.fm_ending-p.fm_starting,0))*15.0 END,2)
    AS commercial_score,

  -- Total
  ROUND((
    IFNULL(ROUND(LEAST(r.fill_rate,1.0)*60,2),0)
    + IFNULL(ROUND(LEAST(r.otd,1.0)*40,2),0)
    + ROUND(CASE WHEN r.yoy_growth IS NULL OR r.yoy_growth<=0 THEN 0.0
        WHEN r.yoy_growth>=y.yoy_max THEN 10.0
        ELSE (r.yoy_growth/y.yoy_max)*10.0 END,2)
    + ROUND(CASE WHEN r.efficiency_ratio IS NULL OR r.efficiency_ratio<0.40 THEN 0.0
        WHEN r.efficiency_ratio>=1.0 THEN 30.0
        ELSE ((r.efficiency_ratio-0.40)/0.60)*30.0 END,2)
    + ROUND(CASE WHEN r.gbd IS NULL OR r.gbd<=0 OR r.gbd<p.gbd_lower THEN 0.0
        WHEN r.gbd<=p.gbd_target THEN ((r.gbd-p.gbd_lower)/NULLIF(p.gbd_target-p.gbd_lower,0))*20.0
        WHEN r.gbd<=p.gbd_upper  THEN (1.0-((r.gbd-p.gbd_target)/NULLIF(p.gbd_upper-p.gbd_target,0))*0.5)*20.0
        ELSE 0.0 END,2)
    + ROUND(CASE WHEN r.has_rebate=0 OR r.back_margin_ratio IS NULL OR r.back_margin_ratio<p.bm_starting THEN 0.0
        WHEN r.back_margin_ratio>=p.bm_ending THEN 25.0
        ELSE ((r.back_margin_ratio-p.bm_starting)/NULLIF(p.bm_ending-p.bm_starting,0))*25.0 END,2)
    + ROUND(CASE WHEN r.front_margin_ratio IS NULL OR r.front_margin_ratio<=0 OR r.front_margin_ratio<p.fm_starting THEN 0.0
        WHEN r.front_margin_ratio>=p.fm_ending THEN 15.0
        ELSE ((r.front_margin_ratio-p.fm_starting)/NULLIF(p.fm_ending-p.fm_starting,0))*15.0 END,2)
  ) / 2.0, 1) AS total_score

FROM ratios r
LEFT JOIN `dh-darkstores-live.csm_automated_tables.ytd_sps_scoring_params` p
  ON  r.global_entity_id = p.global_entity_id
  AND r.time_period      = p.time_period
LEFT JOIN yoy_thresh y
  ON  r.global_entity_id = y.global_entity_id
  AND r.time_period      = y.time_period
  AND r.time_granularity = y.time_granularity
