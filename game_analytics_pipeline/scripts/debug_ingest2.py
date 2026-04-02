"""
Debug script v2: Test different write methods to find what works.
Run: uv run python scripts/debug_ingest2.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
import time

PROJECT = "game-analytics-22"
client = bigquery.Client(project=PROJECT)

# ── Test A: CREATE TABLE in same project (no cross-project) ──
print("=" * 60)
print("TEST A: Write local data (no cross-project)")
print("=" * 60)
q_a = f"""
CREATE OR REPLACE TABLE `{PROJECT}.game_raw.test_local` AS
SELECT 1 AS id, 'hello' AS name
UNION ALL
SELECT 2, 'world'
"""
try:
    client.query(q_a).result()
    r = list(client.query(f"SELECT COUNT(*) AS cnt FROM `{PROJECT}.game_raw.test_local`").result())
    print(f"  Result: {r[0].cnt} rows  (expect 2)")
except Exception as e:
    print(f"  FAILED: {e}")

# ── Test B: CREATE TABLE from cross-project SELECT ──
print("\n" + "=" * 60)
print("TEST B: CREATE TABLE ... AS SELECT from public dataset (LIMIT 10)")
print("=" * 60)
q_b = f"""
CREATE OR REPLACE TABLE `{PROJECT}.game_raw.test_cross` AS
SELECT
    event_name,
    user_pseudo_id,
    platform
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE _TABLE_SUFFIX = '20180901'
LIMIT 10
"""
try:
    job = client.query(q_b)
    job.result()
    print(f"  Job state: {job.state}, errors: {job.errors}")
    r = list(client.query(f"SELECT COUNT(*) AS cnt FROM `{PROJECT}.game_raw.test_cross`").result())
    print(f"  Result: {r[0].cnt} rows  (expect 10)")
except Exception as e:
    print(f"  FAILED: {e}")

# ── Test C: INSERT INTO from cross-project ──
print("\n" + "=" * 60)
print("TEST C: INSERT INTO ... SELECT from public dataset")
print("=" * 60)
q_c1 = f"""
CREATE OR REPLACE TABLE `{PROJECT}.game_raw.test_insert` (
    event_name STRING,
    user_pseudo_id STRING,
    platform STRING
)
"""
q_c2 = f"""
INSERT INTO `{PROJECT}.game_raw.test_insert`
SELECT
    event_name,
    user_pseudo_id,
    platform
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE _TABLE_SUFFIX = '20180901'
LIMIT 10
"""
try:
    client.query(q_c1).result()
    job = client.query(q_c2)
    job.result()
    print(f"  Job state: {job.state}, errors: {job.errors}")
    print(f"  DML stats: {job.num_dml_affected_rows}")
    r = list(client.query(f"SELECT COUNT(*) AS cnt FROM `{PROJECT}.game_raw.test_insert`").result())
    print(f"  Result: {r[0].cnt} rows  (expect 10)")
except Exception as e:
    print(f"  FAILED: {e}")

# ── Test D: Export to dataframe then load ──
print("\n" + "=" * 60)
print("TEST D: Query to DataFrame then load_table_from_dataframe")
print("=" * 60)
try:
    df = client.query("""
        SELECT
            PARSE_DATE('%Y%m%d', event_date) AS event_date,
            event_name,
            user_pseudo_id,
            platform
        FROM `firebase-public-project.analytics_153293282.events_*`
        WHERE _TABLE_SUFFIX = '20180901'
        LIMIT 10
    """).to_dataframe()
    print(f"  DataFrame: {len(df)} rows")

    table_ref = f"{PROJECT}.game_raw.test_df_load"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()
    print(f"  Load job state: {load_job.state}, errors: {load_job.errors}")

    r = list(client.query(f"SELECT COUNT(*) AS cnt FROM `{table_ref}`").result())
    print(f"  Result: {r[0].cnt} rows  (expect 10)")
except Exception as e:
    print(f"  FAILED: {e}")

# ── Summary ──
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
for t in ["test_local", "test_cross", "test_insert", "test_df_load"]:
    try:
        r = list(client.query(f"SELECT COUNT(*) AS cnt FROM `{PROJECT}.game_raw.{t}`").result())
        print(f"  {t}: {r[0].cnt} rows")
    except:
        print(f"  {t}: NOT FOUND")

# Cleanup
print("\nCleaning up test tables...")
for t in ["test_local", "test_cross", "test_insert", "test_df_load"]:
    client.delete_table(f"{PROJECT}.game_raw.{t}", not_found_ok=True)
print("Done.")
