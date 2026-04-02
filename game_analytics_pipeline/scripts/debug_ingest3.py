"""
Debug v3: Test full extraction query at different scales.
Find exactly where it breaks.
Run: uv run python scripts/debug_ingest3.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
import time

PROJECT = "game-analytics-22"
client = bigquery.Client(project=PROJECT)

FULL_SELECT = """
SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_name,
    event_value_in_usd,
    user_pseudo_id,
    user_id,
    TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
    platform,
    stream_id,
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
"""


def test_write(label, where_clause, partition=True):
    table_name = f"{PROJECT}.game_raw.test_scale"

    partition_clause = ""
    if partition:
        partition_clause = "PARTITION BY event_date\nCLUSTER BY event_name, platform, geo_country"

    query = f"""
    CREATE OR REPLACE TABLE `{table_name}`
    {partition_clause}
    AS
    {FULL_SELECT}
    {where_clause}
    """

    print(f"\n  [{label}]")
    start = time.time()
    try:
        job = client.query(query)
        job.result()
        duration = time.time() - start

        r = list(client.query(f"SELECT COUNT(*) AS cnt FROM `{table_name}`").result())
        cnt = r[0].cnt
        gb = (job.total_bytes_processed or 0) / (1024**3)
        print(f"  Rows: {cnt:,}  |  Time: {duration:.1f}s  |  Scanned: {gb:.3f} GB  |  Errors: {job.errors}")
        return cnt
    except Exception as e:
        duration = time.time() - start
        print(f"  FAILED ({duration:.1f}s): {e}")
        return -1


print("=" * 60)
print("TEST 1: Full 34 columns, 1 day, WITH partitioning")
print("=" * 60)
r1 = test_write("1 day + partition", "WHERE _TABLE_SUFFIX = '20180901'", partition=True)

print("\n" + "=" * 60)
print("TEST 2: Full 34 columns, 1 day, NO partitioning")
print("=" * 60)
r2 = test_write("1 day no partition", "WHERE _TABLE_SUFFIX = '20180901'", partition=False)

print("\n" + "=" * 60)
print("TEST 3: Full 34 columns, 1 week, WITH partitioning")
print("=" * 60)
r3 = test_write("1 week + partition", "WHERE _TABLE_SUFFIX BETWEEN '20180901' AND '20180907'", partition=True)

print("\n" + "=" * 60)
print("TEST 4: Full 34 columns, 1 month, WITH partitioning")
print("=" * 60)
r4 = test_write("1 month + partition", "WHERE _TABLE_SUFFIX BETWEEN '20180901' AND '20180930'", partition=True)

if r4 > 0:
    print("\n" + "=" * 60)
    print("TEST 5: Full 34 columns, ALL data, WITH partitioning")
    print("=" * 60)
    r5 = test_write("ALL data + partition", "WHERE _TABLE_SUFFIX BETWEEN '20180612' AND '20181003'", partition=True)

# Cleanup
print("\n\nCleaning up...")
client.delete_table(f"{PROJECT}.game_raw.test_scale", not_found_ok=True)

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  1 day + partition:     {r1:>10,} rows")
print(f"  1 day no partition:    {r2:>10,} rows")
print(f"  1 week + partition:    {r3:>10,} rows")
print(f"  1 month + partition:   {r4:>10,} rows")
if r4 > 0:
    print(f"  ALL data + partition:  {r5:>10,} rows")
