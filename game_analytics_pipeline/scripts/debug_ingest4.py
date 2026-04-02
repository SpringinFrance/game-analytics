"""
Debug v4: Isolate the exact cause - partitioning vs UNNEST subqueries.
Each test drops the table first to avoid conflicts.
Run: uv run python scripts/debug_ingest4.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
import time

PROJECT = "game-analytics-22"
client = bigquery.Client(project=PROJECT)
TABLE = f"{PROJECT}.game_raw.test_isolate"


def drop_and_test(label, query):
    client.delete_table(TABLE, not_found_ok=True)
    time.sleep(1)
    print(f"\n  [{label}]")
    start = time.time()
    try:
        job = client.query(query)
        job.result()
        duration = time.time() - start
        r = list(client.query(f"SELECT COUNT(*) AS cnt FROM `{TABLE}`").result())
        cnt = r[0].cnt
        print(f"  => {cnt:,} rows  |  {duration:.1f}s  |  Errors: {job.errors}")
        return cnt
    except Exception as e:
        print(f"  => FAILED ({time.time()-start:.1f}s): {e}")
        return -1


# ── TEST 1: Simple columns + NO partition ──
print("=" * 60)
print("TEST 1: Simple 3 cols, NO partition, 1 day")
print("=" * 60)
r1 = drop_and_test("simple, no partition", f"""
CREATE TABLE `{TABLE}` AS
SELECT event_name, user_pseudo_id, platform
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE _TABLE_SUFFIX = '20180901'
LIMIT 100
""")

# ── TEST 2: Simple columns + WITH partition ──
print("\n" + "=" * 60)
print("TEST 2: Simple cols + PARTITION BY event_date, 1 day")
print("=" * 60)
r2 = drop_and_test("simple + partition", f"""
CREATE TABLE `{TABLE}`
PARTITION BY event_date
AS
SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_name,
    user_pseudo_id,
    platform
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE _TABLE_SUFFIX = '20180901'
LIMIT 100
""")

# ── TEST 3: Full cols with UNNEST + NO partition ──
print("\n" + "=" * 60)
print("TEST 3: Full 34 cols (UNNEST), NO partition, 1 day")
print("=" * 60)
r3 = drop_and_test("full cols, no partition", f"""
CREATE TABLE `{TABLE}` AS
SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_name, event_value_in_usd,
    user_pseudo_id, user_id,
    TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
    platform, stream_id,
    device.category AS device_category,
    device.mobile_brand_name AS device_brand,
    device.mobile_model_name AS device_model,
    device.operating_system AS device_os,
    device.operating_system_version AS device_os_version,
    device.language AS device_language,
    geo.continent AS geo_continent,
    geo.country AS geo_country,
    geo.region AS geo_region,
    geo.city AS geo_city,
    traffic_source.source AS traffic_source,
    traffic_source.medium AS traffic_medium,
    traffic_source.name AS traffic_campaign,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'level') AS param_level,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'score') AS param_score,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'board') AS param_board,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'success') AS param_success,
    (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') AS param_value,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'virtual_currency_name') AS param_virtual_currency_name,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS param_engagement_time_msec,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'firebase_screen') AS param_firebase_screen,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'firebase_screen_class') AS param_firebase_screen_class,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS param_session_id,
    CURRENT_TIMESTAMP() AS _ingestion_timestamp,
    _TABLE_SUFFIX AS _source_date_suffix
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE _TABLE_SUFFIX = '20180901'
LIMIT 100
""")

# ── TEST 4: Full cols with UNNEST + WITH partition ──
print("\n" + "=" * 60)
print("TEST 4: Full 34 cols (UNNEST) + PARTITION, 1 day")
print("=" * 60)
r4 = drop_and_test("full cols + partition", f"""
CREATE TABLE `{TABLE}`
PARTITION BY event_date
CLUSTER BY event_name, platform, geo_country
AS
SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_name, event_value_in_usd,
    user_pseudo_id, user_id,
    TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
    platform, stream_id,
    device.category AS device_category,
    device.mobile_brand_name AS device_brand,
    device.mobile_model_name AS device_model,
    device.operating_system AS device_os,
    device.operating_system_version AS device_os_version,
    device.language AS device_language,
    geo.continent AS geo_continent,
    geo.country AS geo_country,
    geo.region AS geo_region,
    geo.city AS geo_city,
    traffic_source.source AS traffic_source,
    traffic_source.medium AS traffic_medium,
    traffic_source.name AS traffic_campaign,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'level') AS param_level,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'score') AS param_score,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'board') AS param_board,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'success') AS param_success,
    (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') AS param_value,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'virtual_currency_name') AS param_virtual_currency_name,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS param_engagement_time_msec,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'firebase_screen') AS param_firebase_screen,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'firebase_screen_class') AS param_firebase_screen_class,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS param_session_id,
    CURRENT_TIMESTAMP() AS _ingestion_timestamp,
    _TABLE_SUFFIX AS _source_date_suffix
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE _TABLE_SUFFIX = '20180901'
LIMIT 100
""")

# ── TEST 5: If 3 worked but 4 didn't, try full cols + partition but NO LIMIT ──
if r3 > 0 and r4 == 0:
    print("\n" + "=" * 60)
    print("TEST 5: Full cols + PARTITION, 1 day, NO LIMIT")
    print("=" * 60)
    r5 = drop_and_test("full + partition, no limit", f"""
    CREATE TABLE `{TABLE}`
    PARTITION BY event_date
    CLUSTER BY event_name, platform, geo_country
    AS
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
        event_name, event_value_in_usd,
        user_pseudo_id, user_id,
        TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
        platform, stream_id,
        device.category AS device_category,
        device.mobile_brand_name AS device_brand,
        device.mobile_model_name AS device_model,
        device.operating_system AS device_os,
        device.operating_system_version AS device_os_version,
        device.language AS device_language,
        geo.continent AS geo_continent,
        geo.country AS geo_country,
        geo.region AS geo_region,
        geo.city AS geo_city,
        traffic_source.source AS traffic_source,
        traffic_source.medium AS traffic_medium,
        traffic_source.name AS traffic_campaign,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'level') AS param_level,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'score') AS param_score,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'board') AS param_board,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'success') AS param_success,
        (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') AS param_value,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'virtual_currency_name') AS param_virtual_currency_name,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS param_engagement_time_msec,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'firebase_screen') AS param_firebase_screen,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'firebase_screen_class') AS param_firebase_screen_class,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS param_session_id,
        CURRENT_TIMESTAMP() AS _ingestion_timestamp,
        _TABLE_SUFFIX AS _source_date_suffix
    FROM `firebase-public-project.analytics_153293282.events_*`
    WHERE _TABLE_SUFFIX = '20180901'
    """)

# Cleanup
print("\n\nCleaning up...")
client.delete_table(TABLE, not_found_ok=True)

print("\n" + "=" * 60)
print("VERDICT")
print("=" * 60)
print(f"  Test 1 (simple, no partition):    {r1}")
print(f"  Test 2 (simple + partition):      {r2}")
print(f"  Test 3 (full UNNEST, no part):    {r3}")
print(f"  Test 4 (full UNNEST + partition): {r4}")

if r1 > 0 and r2 == 0:
    print("\n  => CAUSE: Partitioning breaks writes")
elif r1 > 0 and r2 > 0 and r3 == 0:
    print("\n  => CAUSE: UNNEST subqueries break writes")
elif r1 > 0 and r2 > 0 and r3 > 0 and r4 == 0:
    print("\n  => CAUSE: Combination of UNNEST + partitioning breaks writes")
elif r1 > 0 and r2 > 0 and r3 > 0 and r4 > 0:
    print("\n  => All methods work! Issue might be scale-related.")
else:
    print(f"\n  => Unexpected pattern. Check results above.")
