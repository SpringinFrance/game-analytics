-- ============================================================
-- GOOGLE DATA STUDIO - SQL QUERIES FOR DASHBOARDS
-- ============================================================
-- Adapted for Firebase Gaming Public Dataset (Flood-It game)
-- Source: firebase-public-project.analytics_153293282
-- Schema: GA4/Firebase flattened into BigQuery warehouse
--
-- Replace ${PROJECT_ID} with your GCP project ID.
-- Use @DS_START_DATE / @DS_END_DATE for Data Studio date filters.
-- Available date range: 2018-06-12 to 2018-10-03
-- ============================================================


-- ************************************************************
-- DASHBOARD 1: PRODUCT OVERVIEW
-- ************************************************************

-- ── 1.1 DAU/MAU Trend ────────────────────────────────────
SELECT
    date,
    platform,
    dau,
    mau,
    stickiness,
    new_users,
    total_events,
    total_sessions,
    sessions_per_dau,
    avg_session_duration_sec,
    avg_engagement_sec,
    avg_levels_per_session,
FROM `${PROJECT_ID}.game_marts.mart_daily_kpis`
WHERE date BETWEEN @DS_START_DATE AND @DS_END_DATE
ORDER BY date;


-- ── 1.2 Retention Cohort Heatmap ─────────────────────────
SELECT
    cohort_date,
    platform,
    traffic_source,
    cohort_size,
    d1_retention,
    d3_retention,
    d7_retention,
    d14_retention,
    d30_retention,
FROM `${PROJECT_ID}.game_marts.mart_retention_cohorts`
WHERE cohort_date BETWEEN @DS_START_DATE AND @DS_END_DATE
ORDER BY cohort_date DESC;


-- ── 1.3 Session Stats ────────────────────────────────────
SELECT
    date,
    platform,
    total_sessions,
    unique_users,
    avg_duration_sec,
    median_duration_sec,
    avg_engagement_sec,
    avg_levels_per_session,
    sessions_under_1min,
    sessions_1_5min,
    sessions_5_15min,
    sessions_over_15min,
FROM `${PROJECT_ID}.game_marts.mart_session_stats`
WHERE date BETWEEN @DS_START_DATE AND @DS_END_DATE
ORDER BY date;


-- ── 1.4 Level Progression Funnel ─────────────────────────
SELECT
    level_number,
    users_completed,
    total_users,
    completion_rate,
    avg_attempts,
    avg_best_score,
    avg_days_to_complete,
FROM `${PROJECT_ID}.game_marts.mart_level_funnel`
WHERE level_number <= 100
ORDER BY level_number;


-- ── 1.5 Country Breakdown (Geo Map) ─────────────────────
SELECT
    geo_country,
    geo_continent,
    total_users,
    payers,
    total_revenue,
    avg_active_days,
    avg_level,
    avg_engagement_min,
    payer_rate,
FROM `${PROJECT_ID}.game_marts.mart_geo_stats`
ORDER BY total_users DESC;


-- ************************************************************
-- DASHBOARD 2: REVENUE & MONETIZATION
-- ************************************************************

-- ── 2.1 Revenue Trend ────────────────────────────────────
SELECT
    date,
    platform,
    revenue_type,
    unique_payers,
    transaction_count,
    total_revenue,
    avg_transaction_value,
FROM `${PROJECT_ID}.game_marts.mart_revenue_daily`
WHERE date BETWEEN @DS_START_DATE AND @DS_END_DATE
ORDER BY date;


-- ── 2.2 ARPU / ARPPU Trend ──────────────────────────────
SELECT
    date,
    platform,
    arpu,
    arppu,
    dau,
    paying_users,
    total_revenue,
    payer_conversion_rate,
FROM `${PROJECT_ID}.game_marts.mart_daily_kpis`
WHERE date BETWEEN @DS_START_DATE AND @DS_END_DATE
ORDER BY date;


-- ── 2.3 Player Segment Distribution ─────────────────────
SELECT
    player_segment,
    COUNT(*) AS user_count,
    SUM(total_revenue) AS segment_revenue,
    AVG(total_revenue) AS avg_revenue,
    AVG(active_days) AS avg_active_days,
    AVG(max_level) AS avg_level,
    AVG(total_engagement_minutes) AS avg_engagement_min,
FROM `${PROJECT_ID}.game_marts.mart_player_segments`
GROUP BY player_segment
ORDER BY segment_revenue DESC;


-- ── 2.4 LTV Cohort Curves ───────────────────────────────
WITH cohort_revenue AS (
    SELECT
        u.install_date AS cohort_date,
        FORMAT_DATE('%Y-W%V', u.install_date) AS cohort_week,
        DATE_DIFF(r.event_date, u.install_date, DAY) AS days_since_install,
        r.revenue_amount,
        u.user_pseudo_id,
    FROM `${PROJECT_ID}.game_warehouse.dim_users` u
    INNER JOIN `${PROJECT_ID}.game_warehouse.fact_revenue` r
        ON u.user_pseudo_id = r.user_pseudo_id
        AND r.event_date >= u.install_date
)
SELECT
    cohort_week,
    days_since_install,
    COUNT(DISTINCT user_pseudo_id) AS paying_users,
    SUM(revenue_amount) AS cumulative_revenue,
FROM cohort_revenue
WHERE days_since_install BETWEEN 0 AND 30
GROUP BY cohort_week, days_since_install
ORDER BY cohort_week, days_since_install;


-- ************************************************************
-- DASHBOARD 3: GAMING ENGAGEMENT
-- ************************************************************

-- ── 3.1 Level Difficulty Analysis ────────────────────────
SELECT
    level_number,
    level_action,
    COUNT(*) AS action_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    AVG(score) AS avg_score,
FROM `${PROJECT_ID}.game_warehouse.fact_levels`
WHERE level_number IS NOT NULL
GROUP BY level_number, level_action
ORDER BY level_number, level_action;


-- ── 3.2 Player Win Rate by Level ─────────────────────────
SELECT
    level_number,
    COUNTIF(level_action = 'won') AS wins,
    COUNTIF(level_action = 'lost') AS losses,
    COUNTIF(level_action = 'retried') AS retries,
    SAFE_DIVIDE(COUNTIF(level_action = 'won'),
                COUNTIF(level_action IN ('won', 'lost'))) AS win_rate,
FROM `${PROJECT_ID}.game_warehouse.fact_levels`
WHERE level_number IS NOT NULL
GROUP BY level_number
ORDER BY level_number;


-- ── 3.3 Score Distribution by Level ──────────────────────
SELECT
    level_number,
    COUNT(DISTINCT user_pseudo_id) AS players,
    MIN(score) AS min_score,
    APPROX_QUANTILES(score, 4)[OFFSET(1)] AS q25_score,
    APPROX_QUANTILES(score, 4)[OFFSET(2)] AS median_score,
    APPROX_QUANTILES(score, 4)[OFFSET(3)] AS q75_score,
    MAX(score) AS max_score,
    AVG(score) AS avg_score,
FROM `${PROJECT_ID}.game_warehouse.fact_levels`
WHERE event_name = 'post_score'
  AND score IS NOT NULL
  AND level_number IS NOT NULL
GROUP BY level_number
ORDER BY level_number;


-- ── 3.4 Traffic Source Performance ───────────────────────
SELECT
    u.traffic_source,
    u.traffic_medium,
    COUNT(DISTINCT u.user_pseudo_id) AS users,
    AVG(u.active_days) AS avg_active_days,
    AVG(u.max_level) AS avg_level,
    AVG(u.total_engagement_minutes) AS avg_engagement_min,
    SUM(u.total_revenue) AS total_revenue,
    AVG(u.lifetime_days) AS avg_lifetime_days,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN u.days_since_last_active <= 7 THEN u.user_pseudo_id END),
                COUNT(DISTINCT u.user_pseudo_id)) AS retention_rate,
FROM `${PROJECT_ID}.game_warehouse.dim_users` u
GROUP BY u.traffic_source, u.traffic_medium
ORDER BY users DESC;


-- ************************************************************
-- DASHBOARD 4: CHURN PREDICTION
-- ************************************************************

-- ── 4.1 Churn Risk Distribution ─────────────────────────
SELECT
    risk_tier,
    predicted_churn,
    COUNT(*) AS user_count,
    AVG(churn_probability) AS avg_probability,
FROM `${PROJECT_ID}.game_ml.predictions`
GROUP BY risk_tier, predicted_churn
ORDER BY avg_probability DESC;


-- ── 4.2 Churn Risk by Segment ───────────────────────────
SELECT
    s.player_segment,
    p.risk_tier,
    COUNT(*) AS user_count,
    AVG(p.churn_probability) AS avg_churn_prob,
FROM `${PROJECT_ID}.game_ml.predictions` p
JOIN `${PROJECT_ID}.game_ml.player_segments` s
    ON p.user_pseudo_id = s.user_pseudo_id
GROUP BY s.player_segment, p.risk_tier
ORDER BY s.player_segment, avg_churn_prob DESC;


-- ── 4.3 Feature Importance (from model metadata) ────────
SELECT
    model_version,
    trained_at,
    auc_roc,
    f1_score,
    precision,
    recall,
FROM `${PROJECT_ID}.game_ml.model_metadata`
ORDER BY trained_at DESC;
