"""
GCP & BigQuery Setup Verification Script
=========================================
Run this script after completing GCP setup to verify everything works.
It will: check credentials, create datasets, create raw tables,
insert sample data, and validate the entire setup.

Prerequisites:
    1. Install: pip install google-cloud-bigquery google-cloud-storage
    2. Set env var: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
       OR run: gcloud auth application-default login

Usage:
    python scripts/setup_gcp.py --project YOUR_PROJECT_ID
    python scripts/setup_gcp.py --project YOUR_PROJECT_ID --create-sample-data
"""

import argparse
import sys
import os
import json
from datetime import datetime, timedelta

# ── Color output helpers ─────────────────────────────────
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
CHECK = f"{GREEN}\u2713{RESET}"
CROSS = f"{RED}\u2717{RESET}"
WARN = f"{YELLOW}!{RESET}"


def print_header(text):
    print(f"\n{CYAN}{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}{RESET}\n")


def print_ok(text):
    print(f"  {CHECK} {text}")


def print_fail(text):
    print(f"  {CROSS} {text}")


def print_warn(text):
    print(f"  {WARN} {YELLOW}{text}{RESET}")


# ── Step 1: Check credentials ───────────────────────────
def check_credentials(project_id):
    print_header("Step 1: Checking GCP Credentials")

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
        # Try a simple query to verify access
        query = "SELECT 1 AS test"
        result = list(client.query(query).result())
        print_ok(f"Authenticated successfully to project: {project_id}")
        return client
    except Exception as e:
        print_fail(f"Authentication failed: {e}")
        print()
        print("  Please follow these steps:")
        print("  1. Go to https://console.cloud.google.com/iam-admin/serviceaccounts")
        print(f"  2. Select project: {project_id}")
        print("  3. Click '+ CREATE SERVICE ACCOUNT'")
        print("  4. Name: 'game-analytics-pipeline'")
        print("  5. Grant roles: 'BigQuery Admin' + 'Storage Admin'")
        print("  6. Click the service account > Keys > Add Key > JSON")
        print("  7. Download the JSON key file")
        print("  8. Set environment variable:")
        print("     export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json")
        print()
        return None


# ── Step 2: Check/Create datasets ───────────────────────
def check_datasets(client, project_id):
    print_header("Step 2: Checking BigQuery Datasets")

    datasets = {
        "game_raw": "Raw data from AppsFlyer (90-day retention)",
        "game_staging": "Cleaned and deduplicated data (30-day retention)",
        "game_warehouse": "Dimensional model (permanent)",
        "game_marts": "Aggregated KPIs for dashboards (permanent)",
        "game_ml": "ML features, predictions, model metadata (permanent)",
    }

    from google.cloud import bigquery

    all_ok = True
    for name, description in datasets.items():
        dataset_ref = f"{project_id}.{name}"
        try:
            client.get_dataset(dataset_ref)
            print_ok(f"Dataset exists: {name}")
        except Exception:
            try:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                dataset.description = description
                client.create_dataset(dataset)
                print_ok(f"Created dataset: {name} ({description})")
            except Exception as e:
                print_fail(f"Failed to create {name}: {e}")
                all_ok = False

    return all_ok


# ── Step 3: Check/Create raw tables ─────────────────────
def check_raw_tables(client, project_id):
    print_header("Step 3: Checking Raw Tables")

    # Import schemas from the pipeline
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.ingestion.bigquery_loader import BigQueryLoader

    loader = BigQueryLoader(project_id=project_id, dataset="game_raw")

    try:
        loader.create_schemas()
        print_ok("raw_installs table ready (partitioned by install_time)")
        print_ok("raw_events table ready (partitioned by event_time)")
        print_ok("raw_uninstalls table ready (partitioned by event_time)")
        return True
    except Exception as e:
        print_fail(f"Failed to create raw tables: {e}")
        return False


# ── Step 4: Verify table structure ──────────────────────
def verify_tables(client, project_id):
    print_header("Step 4: Verifying Table Schemas")

    tables_to_check = [
        ("game_raw", "raw_installs"),
        ("game_raw", "raw_events"),
        ("game_raw", "raw_uninstalls"),
    ]

    from google.cloud import bigquery

    all_ok = True
    for dataset, table_name in tables_to_check:
        table_ref = f"{project_id}.{dataset}.{table_name}"
        try:
            table = client.get_table(table_ref)
            cols = len(table.schema)
            partition = table.time_partitioning
            clustering = table.clustering_fields or []

            print_ok(f"{table_name}: {cols} columns")
            print(f"      Partition: {partition.field if partition else 'None'}")
            print(f"      Clustering: {', '.join(clustering) if clustering else 'None'}")
            print(f"      Rows: {table.num_rows}")
            print(f"      Size: {table.num_bytes or 0} bytes")
        except Exception as e:
            print_fail(f"{table_name}: {e}")
            all_ok = False

    return all_ok


# ── Step 5: Insert sample data (optional) ───────────────
def insert_sample_data(client, project_id):
    print_header("Step 5: Inserting Sample Data")

    import pandas as pd
    from google.cloud import bigquery

    now = datetime.utcnow()
    yesterday = now - timedelta(days=1)

    # Sample installs
    installs_data = []
    for i in range(100):
        installs_data.append({
            "appsflyer_id": f"af_device_{i:04d}",
            "customer_user_id": f"user_{i:04d}",
            "install_time": (yesterday - timedelta(hours=i % 24)).isoformat(),
            "event_time": (yesterday - timedelta(hours=i % 24)).isoformat(),
            "event_name": "install",
            "media_source": ["facebook_ads", "google_ads", "unity_ads", "organic"][i % 4],
            "campaign": f"campaign_{i % 5}",
            "campaign_id": f"cid_{i % 5}",
            "adset": f"adset_{i % 3}",
            "platform": ["ios", "android"][i % 2],
            "country_code": ["US", "VN", "JP", "KR", "TH"][i % 5],
            "device_type": ["iPhone", "Samsung Galaxy", "Pixel", "iPad"][i % 4],
            "os_version": ["15.0", "14.0", "13.0"][i % 3],
            "app_version": "1.2.0",
            "language": ["en", "vi", "ja", "ko"][i % 4],
            "cost_value": round(1.5 + (i % 10) * 0.3, 2) if i % 4 != 3 else 0,
            "cost_currency": "USD",
            "_ingestion_timestamp": now.isoformat(),
            "_source": "sample_data",
        })

    df_installs = pd.DataFrame(installs_data)
    df_installs["install_time"] = pd.to_datetime(df_installs["install_time"], utc=True)
    df_installs["event_time"] = pd.to_datetime(df_installs["event_time"], utc=True)
    df_installs["_ingestion_timestamp"] = pd.to_datetime(df_installs["_ingestion_timestamp"], utc=True)

    table_ref = f"{project_id}.game_raw.raw_installs"
    job = client.load_table_from_dataframe(df_installs, table_ref)
    job.result()
    print_ok(f"Inserted {len(df_installs)} sample installs")

    # Sample events
    events_data = []
    event_types = [
        ("af_purchase", 4.99),
        ("af_level_achieved", 0),
        ("af_tutorial_completion", 0),
        ("af_ad_view", 0.05),
        ("af_spent_credits", 0),
        ("af_achievement_unlocked", 0),
    ]

    for i in range(500):
        event_name, base_revenue = event_types[i % len(event_types)]
        revenue = base_revenue * (1 + (i % 3) * 0.5) if base_revenue > 0 else 0

        events_data.append({
            "appsflyer_id": f"af_device_{i % 100:04d}",
            "customer_user_id": f"user_{i % 100:04d}",
            "event_time": (yesterday - timedelta(hours=i % 24, minutes=i % 60)).isoformat(),
            "event_name": event_name,
            "event_value": json.dumps({
                "af_revenue": revenue,
                "af_currency": "USD",
                "af_level": i % 50 + 1,
                "af_content_id": f"item_{i % 20}",
            }),
            "event_revenue": revenue,
            "event_revenue_currency": "USD",
            "media_source": ["facebook_ads", "google_ads", "organic"][i % 3],
            "campaign": f"campaign_{i % 5}",
            "platform": ["ios", "android"][i % 2],
            "country_code": ["US", "VN", "JP"][i % 3],
            "device_type": "iPhone" if i % 2 == 0 else "Samsung Galaxy",
            "os_version": "15.0",
            "app_version": "1.2.0",
            "install_time": (yesterday - timedelta(days=i % 30)).isoformat(),
            "_ingestion_timestamp": now.isoformat(),
            "_source": "sample_data",
        })

    df_events = pd.DataFrame(events_data)
    for col in ["event_time", "install_time", "_ingestion_timestamp"]:
        df_events[col] = pd.to_datetime(df_events[col], utc=True)

    table_ref = f"{project_id}.game_raw.raw_events"
    job = client.load_table_from_dataframe(df_events, table_ref)
    job.result()
    print_ok(f"Inserted {len(df_events)} sample events")

    return True


# ── Step 6: Query verification ──────────────────────────
def verify_queries(client, project_id):
    print_header("Step 6: Running Verification Queries")

    queries = {
        "Install count": f"""
            SELECT COUNT(*) AS total_installs,
                   COUNT(DISTINCT appsflyer_id) AS unique_devices,
                   MIN(install_time) AS earliest,
                   MAX(install_time) AS latest
            FROM `{project_id}.game_raw.raw_installs`
        """,
        "Event count by type": f"""
            SELECT event_name,
                   COUNT(*) AS count,
                   SUM(COALESCE(event_revenue, 0)) AS total_revenue
            FROM `{project_id}.game_raw.raw_events`
            GROUP BY event_name
            ORDER BY count DESC
        """,
        "Platform split": f"""
            SELECT platform,
                   COUNT(*) AS installs
            FROM `{project_id}.game_raw.raw_installs`
            GROUP BY platform
        """,
        "Top media sources": f"""
            SELECT media_source,
                   COUNT(*) AS installs,
                   AVG(cost_value) AS avg_cost
            FROM `{project_id}.game_raw.raw_installs`
            GROUP BY media_source
            ORDER BY installs DESC
        """,
    }

    for name, query in queries.items():
        try:
            df = client.query(query).to_dataframe()
            print_ok(f"{name}:")
            # Format output nicely
            for _, row in df.iterrows():
                values = " | ".join(f"{col}={row[col]}" for col in df.columns)
                print(f"      {values}")
        except Exception as e:
            print_fail(f"{name}: {e}")


# ── Main ────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Setup and verify GCP/BigQuery for Game Analytics Pipeline"
    )
    parser.add_argument(
        "--project", required=True, help="GCP Project ID"
    )
    parser.add_argument(
        "--create-sample-data", action="store_true",
        help="Insert sample data for testing"
    )
    args = parser.parse_args()

    print(f"\n{CYAN}Game Analytics Pipeline - GCP Setup Verification{RESET}")
    print(f"Project: {args.project}\n")

    # Step 1: Check credentials
    client = check_credentials(args.project)
    if client is None:
        print(f"\n{RED}Setup failed at Step 1. Fix credentials and try again.{RESET}")
        sys.exit(1)

    # Step 2: Check/create datasets
    check_datasets(client, args.project)

    # Step 3: Create raw tables
    check_raw_tables(client, args.project)

    # Step 4: Verify table structure
    verify_tables(client, args.project)

    # Step 5: Sample data (optional)
    if args.create_sample_data:
        insert_sample_data(client, args.project)

    # Step 6: Verify queries (only if data exists)
    table = client.get_table(f"{args.project}.game_raw.raw_installs")
    if table.num_rows > 0:
        verify_queries(client, args.project)
    else:
        print_warn("No data in tables yet. Run with --create-sample-data to add test data.")

    print_header("Setup Complete!")
    print(f"  {GREEN}Your BigQuery raw tables are ready.{RESET}")
    print(f"  Next steps:")
    print(f"  1. Configure AppsFlyer API token in .env")
    print(f"  2. Run ingestion: python -m src.ingestion.bigquery_loader")
    print(f"  3. Run transforms: python -m src.transformation.run_transforms --project {args.project}")
    print()


if __name__ == "__main__":
    main()
