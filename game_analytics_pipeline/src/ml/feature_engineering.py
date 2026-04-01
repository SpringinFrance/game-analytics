"""
Feature Engineering for Player Segmentation & Churn Prediction
==============================================================
Adapted for GA4/Firebase gaming schema (Flood-It game).
Builds feature vectors from BigQuery warehouse data.

Feature Categories:
    - Activity: session counts, durations, recency
    - Engagement: events per session, level progression, engagement time
    - Monetary: spend amounts, purchase frequency
    - Progression: current level, velocity, win/loss ratio
    - Temporal: account age, day patterns, regularity
    - Trends: 7-day metric slopes (declining = churn signal)
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def build_feature_store_sql(project_id: str, lookback_days: int = 30) -> str:
    """
    Generate SQL to build the ML feature store from warehouse tables.
    Adapted for GA4/Firebase event structure.

    Note: Since the public dataset ends at 2018-10-03, we use that as
    the reference date instead of CURRENT_DATE().
    """
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_ml.feature_store` AS

    -- Reference date: end of dataset
    WITH ref AS (
        SELECT DATE('2018-10-03') AS ref_date
    ),

    -- ═══ ACTIVITY FEATURES ═══
    activity_features AS (
        SELECT
            s.user_pseudo_id,
            COUNT(*) AS sessions_total,
            COUNTIF(s.session_date >= DATE_SUB((SELECT ref_date FROM ref), INTERVAL 3 DAY)) AS sessions_last_3d,
            COUNTIF(s.session_date >= DATE_SUB((SELECT ref_date FROM ref), INTERVAL 7 DAY)) AS sessions_last_7d,
            COUNTIF(s.session_date >= DATE_SUB((SELECT ref_date FROM ref), INTERVAL 14 DAY)) AS sessions_last_14d,

            AVG(s.session_duration_sec) AS avg_session_duration,
            STDDEV(s.session_duration_sec) AS std_session_duration,
            MAX(s.session_duration_sec) AS max_session_duration,
            SUM(s.session_duration_sec) AS total_play_time_sec,

            AVG(s.total_engagement_sec) AS avg_engagement_sec,
            SUM(s.total_engagement_sec) AS total_engagement_sec,

            AVG(s.events_count) AS avg_events_per_session,
            MAX(s.events_count) AS max_events_per_session,

            DATE_DIFF((SELECT ref_date FROM ref), MAX(s.session_date), DAY) AS days_since_last_session,
            COUNT(DISTINCT s.session_date) AS active_days,

        FROM `{project_id}.game_warehouse.fact_sessions` s
        WHERE s.session_date >= DATE_SUB((SELECT ref_date FROM ref), INTERVAL {lookback_days} DAY)
        GROUP BY s.user_pseudo_id
    ),

    -- ═══ ENGAGEMENT / GAMING FEATURES ═══
    engagement_features AS (
        SELECT
            e.user_pseudo_id,
            COUNT(DISTINCT e.event_name) AS unique_event_types,
            COUNT(*) AS total_events,

            -- Level progression
            MAX(e.param_level) AS max_level,
            COUNTIF(e.event_name = 'level_start') AS levels_started,
            COUNTIF(e.event_name = 'level_up') AS levels_completed,
            COUNTIF(e.event_name = 'level_retry') AS levels_retried,
            COUNTIF(e.event_name = 'post_score') AS scores_posted,
            MAX(e.param_score) AS best_score,
            AVG(CASE WHEN e.event_name = 'post_score' THEN e.param_score END) AS avg_score,

            -- Win/Loss ratio from level_end
            SAFE_DIVIDE(
                COUNTIF(e.event_name = 'level_end' AND e.param_success = 'true'),
                NULLIF(COUNTIF(e.event_name = 'level_end'), 0)
            ) AS win_rate,

            -- Retry ratio
            SAFE_DIVIDE(
                COUNTIF(e.event_name = 'level_retry'),
                NULLIF(COUNTIF(e.event_name IN ('level_start', 'level_retry')), 0)
            ) AS retry_rate,

            -- Engagement time
            SUM(e.engagement_time_msec) / 1000.0 AS total_engagement_from_events,

            -- Activity trend: events in last 3d vs previous 3d
            SAFE_DIVIDE(
                COUNTIF(e.event_date >= DATE_SUB((SELECT ref_date FROM ref), INTERVAL 3 DAY)),
                NULLIF(COUNTIF(
                    e.event_date BETWEEN DATE_SUB((SELECT ref_date FROM ref), INTERVAL 6 DAY)
                    AND DATE_SUB((SELECT ref_date FROM ref), INTERVAL 3 DAY)
                ), 0)
            ) AS event_trend_ratio,

        FROM `{project_id}.game_warehouse.fact_events` e
        CROSS JOIN ref
        WHERE e.event_date >= DATE_SUB(ref.ref_date, INTERVAL {lookback_days} DAY)
        GROUP BY e.user_pseudo_id
    ),

    -- ═══ MONETARY FEATURES ═══
    monetary_features AS (
        SELECT
            r.user_pseudo_id,
            SUM(r.revenue_amount) AS total_spend,
            SUM(CASE WHEN r.revenue_type = 'iap' THEN r.revenue_amount ELSE 0 END) AS iap_spend,
            SUM(CASE WHEN r.revenue_type = 'ad' THEN r.revenue_amount ELSE 0 END) AS ad_revenue,
            COUNT(*) AS purchase_count,
            AVG(r.revenue_amount) AS avg_transaction_value,
            MAX(r.revenue_amount) AS max_transaction_value,
            DATE_DIFF((SELECT ref_date FROM ref), MAX(r.event_date), DAY) AS days_since_last_purchase,
        FROM `{project_id}.game_warehouse.fact_revenue` r
        CROSS JOIN ref
        WHERE r.event_date >= DATE_SUB(ref.ref_date, INTERVAL {lookback_days} DAY)
        GROUP BY r.user_pseudo_id
    ),

    -- ═══ SESSION-LEVEL GAMING FEATURES ═══
    session_gaming AS (
        SELECT
            s.user_pseudo_id,
            AVG(s.levels_completed) AS avg_levels_per_session,
            AVG(s.levels_retried) AS avg_retries_per_session,
            MAX(s.max_level_in_session) AS session_max_level,
            AVG(s.best_score) AS avg_best_score_per_session,
        FROM `{project_id}.game_warehouse.fact_sessions` s
        WHERE s.session_date >= DATE_SUB((SELECT ref_date FROM ref), INTERVAL {lookback_days} DAY)
        GROUP BY s.user_pseudo_id
    ),

    -- ═══ USER PROFILE ═══
    user_features AS (
        SELECT
            u.user_pseudo_id,
            u.platform,
            u.geo_country,
            u.geo_continent,
            u.install_date,
            DATE_DIFF((SELECT ref_date FROM ref), u.install_date, DAY) AS account_age_days,
            u.is_payer,
            u.traffic_source,
        FROM `{project_id}.game_warehouse.dim_users` u
    ),

    -- ═══ CHURN LABEL ═══
    -- Churned = no session in the last 7 days of dataset
    churn_labels AS (
        SELECT
            user_pseudo_id,
            CASE
                WHEN MAX(session_date) < DATE_SUB((SELECT ref_date FROM ref), INTERVAL 7 DAY) THEN 1
                ELSE 0
            END AS is_churned,
        FROM `{project_id}.game_warehouse.fact_sessions`
        GROUP BY user_pseudo_id
    )

    -- ═══ COMBINE ═══
    SELECT
        uf.user_pseudo_id,
        uf.platform,
        uf.geo_country,
        uf.geo_continent,
        uf.account_age_days,
        uf.is_payer,
        uf.traffic_source,

        -- Activity
        COALESCE(af.sessions_total, 0) AS sessions_total,
        COALESCE(af.sessions_last_3d, 0) AS sessions_last_3d,
        COALESCE(af.sessions_last_7d, 0) AS sessions_last_7d,
        COALESCE(af.sessions_last_14d, 0) AS sessions_last_14d,
        COALESCE(af.avg_session_duration, 0) AS avg_session_duration,
        COALESCE(af.std_session_duration, 0) AS std_session_duration,
        COALESCE(af.max_session_duration, 0) AS max_session_duration,
        COALESCE(af.total_play_time_sec, 0) AS total_play_time_sec,
        COALESCE(af.avg_engagement_sec, 0) AS avg_engagement_sec,
        COALESCE(af.total_engagement_sec, 0) AS total_engagement_sec,
        COALESCE(af.avg_events_per_session, 0) AS avg_events_per_session,
        COALESCE(af.days_since_last_session, {lookback_days}) AS days_since_last_session,
        COALESCE(af.active_days, 0) AS active_days,

        -- Engagement / Gaming
        COALESCE(ef.unique_event_types, 0) AS unique_event_types,
        COALESCE(ef.total_events, 0) AS total_events,
        COALESCE(ef.max_level, 0) AS max_level,
        COALESCE(ef.levels_started, 0) AS levels_started,
        COALESCE(ef.levels_completed, 0) AS levels_completed,
        COALESCE(ef.levels_retried, 0) AS levels_retried,
        COALESCE(ef.scores_posted, 0) AS scores_posted,
        COALESCE(ef.best_score, 0) AS best_score,
        COALESCE(ef.avg_score, 0) AS avg_score,
        COALESCE(ef.win_rate, 0) AS win_rate,
        COALESCE(ef.retry_rate, 0) AS retry_rate,
        COALESCE(ef.event_trend_ratio, 0) AS event_trend_ratio,

        -- Session gaming
        COALESCE(sg.avg_levels_per_session, 0) AS avg_levels_per_session,
        COALESCE(sg.avg_retries_per_session, 0) AS avg_retries_per_session,
        COALESCE(sg.avg_best_score_per_session, 0) AS avg_best_score_per_session,

        -- Monetary
        COALESCE(mf.total_spend, 0) AS total_spend,
        COALESCE(mf.iap_spend, 0) AS iap_spend,
        COALESCE(mf.ad_revenue, 0) AS ad_revenue,
        COALESCE(mf.purchase_count, 0) AS purchase_count,
        COALESCE(mf.avg_transaction_value, 0) AS avg_transaction_value,
        COALESCE(mf.days_since_last_purchase, {lookback_days}) AS days_since_last_purchase,

        -- Derived ratios
        SAFE_DIVIDE(af.sessions_last_3d, NULLIF(af.sessions_last_7d, 0)) AS session_acceleration,
        SAFE_DIVIDE(af.active_days, NULLIF(uf.account_age_days, 0)) AS activity_rate,
        SAFE_DIVIDE(ef.levels_completed, NULLIF(uf.account_age_days, 0)) AS level_velocity,
        SAFE_DIVIDE(ef.levels_completed, NULLIF(ef.levels_started, 0)) AS level_completion_rate,

        -- Label
        COALESCE(cl.is_churned, 1) AS is_churned,

        CURRENT_TIMESTAMP() AS feature_computed_at,

    FROM user_features uf
    LEFT JOIN activity_features af ON uf.user_pseudo_id = af.user_pseudo_id
    LEFT JOIN engagement_features ef ON uf.user_pseudo_id = ef.user_pseudo_id
    LEFT JOIN monetary_features mf ON uf.user_pseudo_id = mf.user_pseudo_id
    LEFT JOIN session_gaming sg ON uf.user_pseudo_id = sg.user_pseudo_id
    LEFT JOIN churn_labels cl ON uf.user_pseudo_id = cl.user_pseudo_id
    """


def get_feature_columns() -> list:
    """Return feature column names for ML models."""
    return [
        # Activity
        "sessions_total", "sessions_last_3d", "sessions_last_7d", "sessions_last_14d",
        "avg_session_duration", "std_session_duration", "max_session_duration",
        "total_play_time_sec", "avg_engagement_sec", "total_engagement_sec",
        "avg_events_per_session", "days_since_last_session", "active_days",
        # Engagement / Gaming
        "unique_event_types", "total_events", "max_level",
        "levels_started", "levels_completed", "levels_retried",
        "scores_posted", "best_score", "avg_score",
        "win_rate", "retry_rate", "event_trend_ratio",
        # Session gaming
        "avg_levels_per_session", "avg_retries_per_session", "avg_best_score_per_session",
        # Monetary
        "total_spend", "iap_spend", "ad_revenue", "purchase_count",
        "avg_transaction_value", "days_since_last_purchase",
        # Derived
        "session_acceleration", "activity_rate", "level_velocity", "level_completion_rate",
        # Profile
        "account_age_days",
    ]
