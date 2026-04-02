"""
Debug script: test each step of the ingest process individually.
Run: uv run python scripts/debug_ingest.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery

PROJECT = "game-analytics-22"
client = bigquery.Client(project=PROJECT)

# ── Step 1: Can we read from the public dataset at all? ──
print("=" * 60)
print("STEP 1: Test reading from Firebase public dataset")
print("=" * 60)
q1 = """
SELECT COUNT(*) AS cnt
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20180612' AND '20181003'
"""
try:
    result = client.query(q1).result()
    for row in result:
        print(f"  Source row count: {row.cnt:,}")
except Exception as e:
    print(f"  FAILED: {e}")
    sys.exit(1)

# ── Step 2: Test the extraction query (no destination, just LIMIT 5) ──
print("\n" + "=" * 60)
print("STEP 2: Test extraction query (LIMIT 5, no destination)")
print("=" * 60)
q2 = """
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
    _TABLE_SUFFIX AS _source_date_suffix,
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20180612' AND '20181003'
LIMIT 5
"""
try:
    df = client.query(q2).to_dataframe()
    print(f"  Rows returned: {len(df)}")
    print(f"  Columns ({len(df.columns)}): {list(df.columns)}")
    if len(df) > 0:
        print(f"\n  Sample row:")
        for col in df.columns:
            print(f"    {col}: {df.iloc[0][col]}")
except Exception as e:
    print(f"  FAILED: {e}")
    sys.exit(1)

# ── Step 3: Test writing to destination with WRITE_TRUNCATE ──
print("\n" + "=" * 60)
print("STEP 3: Test write to game_raw.raw_events (small batch)")
print("=" * 60)

# First delete existing table
dest = f"{PROJECT}.game_raw.raw_events"
client.delete_table(dest, not_found_ok=True)
print(f"  Deleted existing table (if any)")

# Write with LIMIT 100
q3 = q2.replace("LIMIT 5", "LIMIT 100")

job_config = bigquery.QueryJobConfig(
    destination=dest,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    time_partitioning=bigquery.TimePartitioning(
        field="event_date",
        type_=bigquery.TimePartitioningType.DAY,
    ),
    clustering_fields=["event_name", "platform", "geo_country"],
)

try:
    print(f"  Running query with destination={dest} ...")
    job = client.query(q3, job_config=job_config)
    job.result()  # wait

    print(f"  Job state: {job.state}")
    print(f"  Job errors: {job.errors}")
    print(f"  Bytes processed: {job.total_bytes_processed:,}")

    # Check the table
    table = client.get_table(dest)
    print(f"  Table num_rows: {table.num_rows}")
    print(f"  Table num_bytes: {table.num_bytes}")
    print(f"  Table schema columns: {len(table.schema)}")

    # Query it back
    verify = client.query(f"SELECT COUNT(*) AS cnt FROM `{dest}`").result()
    for row in verify:
        print(f"  SELECT COUNT(*) confirms: {row.cnt} rows")

except Exception as e:
    print(f"  FAILED: {e}")
    import traceback
    traceback.print_exc()

# ── Step 4: If step 3 worked, do full load ──
print("\n" + "=" * 60)
print("STEP 4: Result")
print("=" * 60)
try:
    table = client.get_table(dest)
    if table.num_rows and table.num_rows > 0:
        print(f"  SUCCESS - Test write worked ({table.num_rows} rows)")
        print(f"  Ready for full ingest. Run:")
        print(f"    uv run python scripts/run_pipeline.py --project {PROJECT} --run ingest")
    else:
        print(f"  PROBLEM - Table exists but has 0 rows.")
        print(f"  This might be a permissions issue or billing issue.")
        print(f"  Try running this query directly in BigQuery console:")
        print(f"  SELECT COUNT(*) FROM `firebase-public-project.analytics_153293282.events_*`")
except Exception as e:
    print(f"  Could not check table: {e}")
