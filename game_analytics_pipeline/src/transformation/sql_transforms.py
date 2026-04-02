"""
Data Transformation SQL Queries (GA4/Firebase Schema)
=====================================================
All SQL transformations adapted for the Firebase Gaming Public Dataset.
Source: firebase-public-project.analytics_153293282 (Flood-It game)

Layers:
  - Raw → Staging: Clean, deduplicate, compute session-level data
  - Staging → Warehouse: Star schema (facts + dimensions)
  - Warehouse → Data Marts: Aggregated KPIs for dashboards
"""


def get_all_transforms(project_id: str) -> dict:
    """Return all SQL transforms organized by layer."""
    return {
        "staging": {
            "stg_events": staging_events(project_id),
            "stg_sessions": staging_sessions(project_id),
        },
        "warehouse": {
            "dim_dates": dim_dates(project_id),
            "dim_users": dim_users(project_id),
            "fact_events": fact_events(project_id),
            "fact_sessions": fact_sessions(project_id),
            "fact_revenue": fact_revenue(project_id),
            "fact_levels": fact_levels(project_id),
        },
        "marts": {
            "mart_daily_kpis": mart_daily_kpis(project_id),
            "mart_retention_cohorts": mart_retention_cohorts(project_id),
            "mart_revenue_daily": mart_revenue_daily(project_id),
            "mart_player_segments": mart_player_segments(project_id),
            "mart_session_stats": mart_session_stats(project_id),
            "mart_level_funnel": mart_level_funnel(project_id),
            "mart_geo_stats": mart_geo_stats(project_id),
        },
    }


# ════════════════════════════════════════════════════════════
# STAGING LAYER
# ════════════════════════════════════════════════════════════

def staging_events(project_id: str) -> str:
    """Clean and deduplicate raw events."""
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_staging.stg_events`
    AS
    SELECT
        -- Unique event ID
        FARM_FINGERPRINT(
            CONCAT(
                COALESCE(user_pseudo_id, ''),
                '|', CAST(event_timestamp AS STRING),
                '|', event_name,
                '|', COALESCE(CAST(param_level AS STRING), ''),
                '|', COALESCE(CAST(param_score AS STRING), '')
            )
        ) AS event_id,

        -- User
        user_pseudo_id,
        COALESCE(user_id, user_pseudo_id) AS user_id,
        user_first_touch_timestamp,

        -- Event
        event_date,
        event_timestamp,
        event_name,
        COALESCE(event_value_in_usd, 0) AS event_value_usd,

        -- Platform & Device
        LOWER(platform) AS platform,
        COALESCE(device_category, 'unknown') AS device_category,
        COALESCE(device_brand, 'unknown') AS device_brand,
        device_model,
        device_os,
        device_os_version,
        COALESCE(device_language, 'en') AS device_language,

        -- Geo
        COALESCE(geo_continent, 'unknown') AS geo_continent,
        COALESCE(geo_country, 'unknown') AS geo_country,
        geo_region,
        geo_city,

        -- Traffic
        COALESCE(traffic_source, 'direct') AS traffic_source,
        COALESCE(traffic_medium, 'none') AS traffic_medium,
        traffic_campaign,

        -- Gaming params
        param_level,
        param_score,
        param_board,
        param_success,
        param_value,
        param_virtual_currency_name,
        COALESCE(param_engagement_time_msec, 0) AS engagement_time_msec,
        param_firebase_screen,
        param_session_id,

    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY user_pseudo_id, event_timestamp, event_name
                ORDER BY _ingestion_timestamp DESC
            ) AS _row_num
        FROM `{project_id}.game_raw.raw_events`
        WHERE user_pseudo_id IS NOT NULL
          AND event_timestamp IS NOT NULL
    )
    WHERE _row_num = 1
    """


def staging_sessions(project_id: str) -> str:
    """Build session-level aggregations from events using session_start events and 30-min gap logic."""
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_staging.stg_sessions`
    AS
    WITH event_gaps AS (
        SELECT
            user_pseudo_id,
            user_id,
            event_timestamp,
            event_date,
            event_name,
            event_value_usd,
            platform,
            geo_country,
            engagement_time_msec,
            param_level,
            TIMESTAMP_DIFF(
                event_timestamp,
                LAG(event_timestamp) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp),
                MINUTE
            ) AS minutes_since_prev,
        FROM `{project_id}.game_staging.stg_events`
    ),
    session_boundaries AS (
        SELECT *,
            SUM(CASE WHEN minutes_since_prev IS NULL OR minutes_since_prev > 30 THEN 1 ELSE 0 END)
                OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS session_num,
        FROM event_gaps
    )
    SELECT
        FARM_FINGERPRINT(CONCAT(user_pseudo_id, '|', CAST(session_num AS STRING), '|', CAST(MIN(event_timestamp) AS STRING))) AS session_id,
        user_pseudo_id,
        ANY_VALUE(user_id) AS user_id,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end,
        MIN(event_date) AS session_date,
        TIMESTAMP_DIFF(MAX(event_timestamp), MIN(event_timestamp), SECOND) AS session_duration_sec,
        SUM(engagement_time_msec) / 1000 AS total_engagement_sec,
        COUNT(*) AS events_count,
        COUNT(DISTINCT event_name) AS unique_event_types,
        SUM(event_value_usd) AS session_revenue,
        MAX(param_level) AS max_level_in_session,

        -- Gaming-specific
        COUNTIF(event_name = 'level_start') AS levels_started,
        COUNTIF(event_name = 'level_up') AS levels_completed,
        COUNTIF(event_name = 'level_end') AS levels_ended,
        COUNTIF(event_name = 'level_retry') AS levels_retried,
        COUNTIF(event_name = 'post_score') AS scores_posted,
        MAX(CASE WHEN event_name = 'post_score' THEN param_score END) AS best_score,

        ANY_VALUE(platform) AS platform,
        ANY_VALUE(geo_country) AS geo_country,

    FROM session_boundaries
    GROUP BY user_pseudo_id, session_num
    """


# ════════════════════════════════════════════════════════════
# WAREHOUSE LAYER
# ════════════════════════════════════════════════════════════

def dim_dates(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_warehouse.dim_dates` AS
    SELECT
        date AS date_key,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(QUARTER FROM date) AS quarter,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(WEEK FROM date) AS week_of_year,
        EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
        FORMAT_DATE('%A', date) AS day_name,
        FORMAT_DATE('%B', date) AS month_name,
        FORMAT_DATE('%Y-W%V', date) AS year_week,
        FORMAT_DATE('%Y-%m', date) AS year_month,
        CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    FROM UNNEST(GENERATE_DATE_ARRAY('2018-01-01', '2019-12-31', INTERVAL 1 DAY)) AS date
    """


def dim_users(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_warehouse.dim_users`
    AS
    WITH first_events AS (
        SELECT
            user_pseudo_id,
            user_id,
            MIN(event_date) AS first_seen_date,
            MIN(event_timestamp) AS first_seen_timestamp,
            ANY_VALUE(user_first_touch_timestamp) AS first_touch_timestamp,
        FROM `{project_id}.game_staging.stg_events`
        GROUP BY user_pseudo_id, user_id
    ),
    user_activity AS (
        SELECT
            user_pseudo_id,
            MAX(event_date) AS last_active_date,
            COUNT(DISTINCT event_date) AS active_days,
            SUM(event_value_usd) AS total_revenue,
            COUNT(CASE WHEN event_name IN ('in_app_purchase', 'spend_virtual_currency') AND event_value_usd > 0 THEN 1 END) AS purchase_count,
            MAX(param_level) AS max_level,
            SUM(engagement_time_msec) / 1000.0 / 60.0 AS total_engagement_minutes,
            COUNT(*) AS total_events,
        FROM `{project_id}.game_staging.stg_events`
        GROUP BY user_pseudo_id
    ),
    user_first_device AS (
        SELECT DISTINCT
            user_pseudo_id,
            FIRST_VALUE(platform) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS platform,
            FIRST_VALUE(device_category) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS device_category,
            FIRST_VALUE(device_brand) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS device_brand,
            FIRST_VALUE(device_os) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS device_os,
            FIRST_VALUE(geo_country) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS geo_country,
            FIRST_VALUE(geo_continent) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS geo_continent,
            FIRST_VALUE(traffic_source) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS traffic_source,
            FIRST_VALUE(traffic_medium) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS traffic_medium,
            FIRST_VALUE(traffic_campaign) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS traffic_campaign,
        FROM `{project_id}.game_staging.stg_events`
    )
    SELECT
        fe.user_pseudo_id,
        fe.user_id,
        fe.first_seen_date AS install_date,
        fe.first_touch_timestamp,

        -- Device & Geo (first-touch)
        d.platform,
        d.device_category,
        d.device_brand,
        d.device_os,
        d.geo_country,
        d.geo_continent,

        -- Attribution (first-touch)
        d.traffic_source,
        d.traffic_medium,
        d.traffic_campaign,

        -- Activity
        ua.last_active_date,
        ua.active_days,
        ua.total_revenue,
        ua.purchase_count,
        ua.max_level,
        ua.total_engagement_minutes,
        ua.total_events,

        -- Derived
        DATE_DIFF(ua.last_active_date, fe.first_seen_date, DAY) AS lifetime_days,
        DATE_DIFF(DATE('2018-10-03'), ua.last_active_date, DAY) AS days_since_last_active,
        ua.total_revenue > 0 AS is_payer,

        -- ML placeholders
        CAST(NULL AS STRING) AS player_segment,
        CAST(NULL AS FLOAT64) AS churn_probability,
        CAST(NULL AS FLOAT64) AS ltv_predicted,

    FROM first_events fe
    LEFT JOIN user_activity ua ON fe.user_pseudo_id = ua.user_pseudo_id
    LEFT JOIN (SELECT DISTINCT * FROM user_first_device) d ON fe.user_pseudo_id = d.user_pseudo_id
    """


def fact_events(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_warehouse.fact_events`
    AS
    SELECT
        event_id,
        user_pseudo_id,
        user_id,
        event_date,
        event_timestamp,
        event_name,
        event_value_usd,
        platform,
        geo_country,
        geo_continent,
        traffic_source,
        traffic_medium,
        param_level,
        param_score,
        param_board,
        param_success,
        engagement_time_msec,
    FROM `{project_id}.game_staging.stg_events`
    """


def fact_sessions(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_warehouse.fact_sessions`
    AS
    SELECT * FROM `{project_id}.game_staging.stg_sessions`
    """


def fact_revenue(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_warehouse.fact_revenue`
    AS
    SELECT
        event_id,
        user_pseudo_id,
        user_id,
        event_date,
        event_timestamp,
        event_name,
        event_value_usd AS revenue_amount,
        CASE
            WHEN event_name = 'in_app_purchase' THEN 'iap'
            WHEN event_name IN ('ad_impression', 'ad_click') THEN 'ad'
            WHEN event_name = 'earn_virtual_currency' THEN 'virtual'
            ELSE 'other'
        END AS revenue_type,
        platform,
        geo_country,
        traffic_source,
        param_virtual_currency_name,
    FROM `{project_id}.game_staging.stg_events`
    WHERE event_value_usd > 0
       OR event_name IN ('in_app_purchase', 'ad_impression', 'ad_click',
                         'spend_virtual_currency', 'earn_virtual_currency')
    """


def fact_levels(project_id: str) -> str:
    """Gaming-specific: level progression facts."""
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_warehouse.fact_levels`
    AS
    SELECT
        event_id,
        user_pseudo_id,
        user_id,
        event_date,
        event_timestamp,
        event_name,
        param_level AS level_number,
        param_score AS score,
        param_board AS board,
        param_success AS success,
        platform,
        geo_country,

        -- Level outcome
        CASE
            WHEN event_name = 'level_start' THEN 'started'
            WHEN event_name = 'level_up' THEN 'completed'
            WHEN event_name = 'level_end' AND param_success = 'true' THEN 'won'
            WHEN event_name = 'level_end' AND param_success = 'false' THEN 'lost'
            WHEN event_name = 'level_retry' THEN 'retried'
            WHEN event_name = 'level_reset' THEN 'reset'
            ELSE event_name
        END AS level_action,

    FROM `{project_id}.game_staging.stg_events`
    WHERE event_name IN ('level_start', 'level_up', 'level_end', 'level_retry',
                         'level_reset', 'level_complete', 'post_score')
    """


# ════════════════════════════════════════════════════════════
# DATA MARTS
# ════════════════════════════════════════════════════════════

def mart_daily_kpis(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_marts.mart_daily_kpis`
    AS
    WITH daily_active AS (
        SELECT
            event_date AS date,
            platform,
            COUNT(DISTINCT user_pseudo_id) AS dau,
            COUNT(DISTINCT CASE WHEN event_value_usd > 0 THEN user_pseudo_id END) AS paying_users,
            SUM(event_value_usd) AS total_revenue,
            COUNT(*) AS total_events,
            SUM(engagement_time_msec) / 1000.0 AS total_engagement_sec,
        FROM `{project_id}.game_warehouse.fact_events`
        GROUP BY event_date, platform
    ),
    monthly_active AS (
        SELECT
            d.date,
            d.platform,
            COUNT(DISTINCT e.user_pseudo_id) AS mau,
        FROM daily_active d
        JOIN `{project_id}.game_warehouse.fact_events` e
            ON e.event_date BETWEEN DATE_SUB(d.date, INTERVAL 29 DAY) AND d.date
            AND e.platform = d.platform
        GROUP BY d.date, d.platform
    ),
    daily_sessions AS (
        SELECT
            session_date AS date,
            platform,
            COUNT(*) AS total_sessions,
            AVG(session_duration_sec) AS avg_session_duration_sec,
            AVG(total_engagement_sec) AS avg_engagement_sec,
            AVG(events_count) AS avg_events_per_session,
            AVG(levels_completed) AS avg_levels_per_session,
        FROM `{project_id}.game_warehouse.fact_sessions`
        GROUP BY session_date, platform
    ),
    daily_new_users AS (
        SELECT
            install_date AS date,
            platform,
            COUNT(*) AS new_users,
        FROM `{project_id}.game_warehouse.dim_users`
        GROUP BY install_date, platform
    )
    SELECT
        da.date,
        da.platform,
        da.dau,
        COALESCE(ma.mau, 0) AS mau,
        SAFE_DIVIDE(da.dau, ma.mau) AS stickiness,
        da.paying_users,
        da.total_revenue,
        SAFE_DIVIDE(da.total_revenue, da.dau) AS arpu,
        SAFE_DIVIDE(da.total_revenue, NULLIF(da.paying_users, 0)) AS arppu,
        SAFE_DIVIDE(da.paying_users, da.dau) AS payer_conversion_rate,
        da.total_events,
        COALESCE(ds.total_sessions, 0) AS total_sessions,
        SAFE_DIVIDE(ds.total_sessions, da.dau) AS sessions_per_dau,
        COALESCE(ds.avg_session_duration_sec, 0) AS avg_session_duration_sec,
        COALESCE(ds.avg_engagement_sec, 0) AS avg_engagement_sec,
        COALESCE(ds.avg_events_per_session, 0) AS avg_events_per_session,
        COALESCE(ds.avg_levels_per_session, 0) AS avg_levels_per_session,
        COALESCE(dnu.new_users, 0) AS new_users,
    FROM daily_active da
    LEFT JOIN monthly_active ma ON da.date = ma.date AND da.platform = ma.platform
    LEFT JOIN daily_sessions ds ON da.date = ds.date AND da.platform = ds.platform
    LEFT JOIN daily_new_users dnu ON da.date = dnu.date AND da.platform = dnu.platform
    """


def mart_retention_cohorts(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_marts.mart_retention_cohorts` AS
    WITH cohorts AS (
        SELECT
            user_pseudo_id,
            install_date AS cohort_date,
            platform,
            traffic_source,
        FROM `{project_id}.game_warehouse.dim_users`
    ),
    user_activity AS (
        SELECT DISTINCT
            user_pseudo_id,
            event_date AS active_date,
        FROM `{project_id}.game_warehouse.fact_events`
    ),
    retention_raw AS (
        SELECT
            c.cohort_date,
            c.platform,
            c.traffic_source,
            c.user_pseudo_id,
            DATE_DIFF(ua.active_date, c.cohort_date, DAY) AS days_since_install,
        FROM cohorts c
        INNER JOIN user_activity ua ON c.user_pseudo_id = ua.user_pseudo_id
        WHERE ua.active_date >= c.cohort_date
    )
    SELECT
        cohort_date,
        platform,
        traffic_source,
        COUNT(DISTINCT CASE WHEN days_since_install = 0 THEN user_pseudo_id END) AS cohort_size,
        COUNT(DISTINCT CASE WHEN days_since_install = 1 THEN user_pseudo_id END) AS d1_users,
        COUNT(DISTINCT CASE WHEN days_since_install = 3 THEN user_pseudo_id END) AS d3_users,
        COUNT(DISTINCT CASE WHEN days_since_install = 7 THEN user_pseudo_id END) AS d7_users,
        COUNT(DISTINCT CASE WHEN days_since_install = 14 THEN user_pseudo_id END) AS d14_users,
        COUNT(DISTINCT CASE WHEN days_since_install = 30 THEN user_pseudo_id END) AS d30_users,
        SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN days_since_install = 1 THEN user_pseudo_id END),
                     COUNT(DISTINCT CASE WHEN days_since_install = 0 THEN user_pseudo_id END)) AS d1_retention,
        SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN days_since_install = 3 THEN user_pseudo_id END),
                     COUNT(DISTINCT CASE WHEN days_since_install = 0 THEN user_pseudo_id END)) AS d3_retention,
        SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN days_since_install = 7 THEN user_pseudo_id END),
                     COUNT(DISTINCT CASE WHEN days_since_install = 0 THEN user_pseudo_id END)) AS d7_retention,
        SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN days_since_install = 14 THEN user_pseudo_id END),
                     COUNT(DISTINCT CASE WHEN days_since_install = 0 THEN user_pseudo_id END)) AS d14_retention,
        SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN days_since_install = 30 THEN user_pseudo_id END),
                     COUNT(DISTINCT CASE WHEN days_since_install = 0 THEN user_pseudo_id END)) AS d30_retention,
    FROM retention_raw
    GROUP BY cohort_date, platform, traffic_source
    ORDER BY cohort_date DESC
    """


def mart_revenue_daily(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_marts.mart_revenue_daily`
    AS
    SELECT
        event_date AS date,
        platform,
        revenue_type,
        COUNT(DISTINCT user_pseudo_id) AS unique_payers,
        COUNT(*) AS transaction_count,
        SUM(revenue_amount) AS total_revenue,
        AVG(revenue_amount) AS avg_transaction_value,
    FROM `{project_id}.game_warehouse.fact_revenue`
    GROUP BY event_date, platform, revenue_type
    """


def mart_player_segments(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_marts.mart_player_segments` AS
    WITH user_metrics AS (
        SELECT
            u.user_pseudo_id,
            u.platform,
            u.geo_country,
            u.install_date,
            u.total_revenue,
            u.purchase_count,
            u.active_days,
            u.days_since_last_active,
            u.lifetime_days,
            u.max_level,
            u.total_engagement_minutes,
            u.total_events,

            NTILE(4) OVER (ORDER BY u.days_since_last_active DESC) AS recency_score,
            NTILE(4) OVER (ORDER BY u.active_days ASC) AS frequency_score,
            NTILE(4) OVER (ORDER BY u.total_revenue ASC) AS monetary_score,

        FROM `{project_id}.game_warehouse.dim_users` u
        WHERE u.lifetime_days >= 0
    )
    SELECT
        *,
        (recency_score + frequency_score + monetary_score) AS rfm_total,

        CASE
            WHEN total_revenue > 0 AND monetary_score = 4 AND purchase_count >= 5
                THEN 'whale'
            WHEN total_revenue > 0 AND monetary_score >= 3
                THEN 'dolphin'
            WHEN total_revenue > 0
                THEN 'minnow'
            WHEN days_since_last_active > 14
                THEN 'churned'
            WHEN days_since_last_active > 7 AND recency_score <= 2
                THEN 'at_risk'
            WHEN frequency_score >= 3 AND active_days >= 5
                THEN 'engaged_free'
            ELSE 'casual'
        END AS player_segment,

    FROM user_metrics
    """


def mart_session_stats(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_marts.mart_session_stats`
    AS
    SELECT
        session_date AS date,
        platform,
        COUNT(*) AS total_sessions,
        COUNT(DISTINCT user_pseudo_id) AS unique_users,
        AVG(session_duration_sec) AS avg_duration_sec,
        APPROX_QUANTILES(session_duration_sec, 100)[OFFSET(50)] AS median_duration_sec,
        AVG(total_engagement_sec) AS avg_engagement_sec,
        AVG(events_count) AS avg_events_per_session,
        AVG(levels_completed) AS avg_levels_per_session,
        SUM(session_revenue) AS total_session_revenue,
        -- Session length distribution
        COUNTIF(session_duration_sec < 60) AS sessions_under_1min,
        COUNTIF(session_duration_sec BETWEEN 60 AND 300) AS sessions_1_5min,
        COUNTIF(session_duration_sec BETWEEN 300 AND 900) AS sessions_5_15min,
        COUNTIF(session_duration_sec > 900) AS sessions_over_15min,
    FROM `{project_id}.game_warehouse.fact_sessions`
    GROUP BY session_date, platform
    """


def mart_level_funnel(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_marts.mart_level_funnel` AS
    WITH total_users AS (
        SELECT COUNT(DISTINCT user_pseudo_id) AS total
        FROM `{project_id}.game_warehouse.dim_users`
    ),
    level_completions AS (
        SELECT
            user_pseudo_id,
            level_number,
            MIN(event_date) AS first_completed_date,
            COUNT(*) AS attempts,
            MAX(score) AS best_score,
        FROM `{project_id}.game_warehouse.fact_levels`
        WHERE event_name IN ('level_up', 'level_complete', 'level_end')
          AND (param_success IS NULL OR param_success = 'true')
          AND level_number IS NOT NULL
        GROUP BY user_pseudo_id, level_number
    )
    SELECT
        lc.level_number,
        COUNT(DISTINCT lc.user_pseudo_id) AS users_completed,
        (SELECT total FROM total_users) AS total_users,
        SAFE_DIVIDE(COUNT(DISTINCT lc.user_pseudo_id), (SELECT total FROM total_users)) AS completion_rate,
        AVG(lc.attempts) AS avg_attempts,
        AVG(lc.best_score) AS avg_best_score,
        AVG(DATE_DIFF(lc.first_completed_date, u.install_date, DAY)) AS avg_days_to_complete,
    FROM level_completions lc
    JOIN `{project_id}.game_warehouse.dim_users` u ON lc.user_pseudo_id = u.user_pseudo_id
    GROUP BY lc.level_number
    ORDER BY lc.level_number
    """


def mart_geo_stats(project_id: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{project_id}.game_marts.mart_geo_stats` AS
    SELECT
        geo_country,
        geo_continent,
        COUNT(DISTINCT user_pseudo_id) AS total_users,
        COUNT(DISTINCT CASE WHEN is_payer THEN user_pseudo_id END) AS payers,
        SUM(total_revenue) AS total_revenue,
        AVG(active_days) AS avg_active_days,
        AVG(max_level) AS avg_level,
        AVG(total_engagement_minutes) AS avg_engagement_min,
        SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN is_payer THEN user_pseudo_id END),
                     COUNT(DISTINCT user_pseudo_id)) AS payer_rate,
    FROM `{project_id}.game_warehouse.dim_users`
    GROUP BY geo_country, geo_continent
    ORDER BY total_users DESC
    """
