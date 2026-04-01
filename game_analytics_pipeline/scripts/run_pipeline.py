"""
End-to-End Pipeline Runner
===========================
Runs the complete game analytics pipeline from data ingestion to ML predictions.
Uses the Firebase Gaming Public Dataset (Flood-It game) as data source.

Steps:
    1. Explore source dataset (optional)
    2. Ingest data: Firebase Public Dataset → BigQuery raw tables
    3. Transform: raw → staging → warehouse → data marts
    4. ML: Build features → Train churn model → Segment players
    5. Verify: Run validation queries on all layers

Usage:
    # Full pipeline
    python scripts/run_pipeline.py --project YOUR_PROJECT_ID --run all

    # Individual steps
    python scripts/run_pipeline.py --project YOUR_PROJECT_ID --run explore
    python scripts/run_pipeline.py --project YOUR_PROJECT_ID --run ingest
    python scripts/run_pipeline.py --project YOUR_PROJECT_ID --run transform
    python scripts/run_pipeline.py --project YOUR_PROJECT_ID --run ml
    python scripts/run_pipeline.py --project YOUR_PROJECT_ID --run verify

    # Ingest specific date range
    python scripts/run_pipeline.py --project YOUR_PROJECT_ID --run ingest --date-from 2018-07-01 --date-to 2018-07-31
"""

import argparse
import logging
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Color output ────────────────────────────────────────
GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RESET = "\033[0m"
CHECK = f"{GREEN}\u2713{RESET}"
CROSS = f"{RED}\u2717{RESET}"


def banner(text):
    print(f"\n{CYAN}{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}{RESET}\n")


# ════════════════════════════════════════════════════════════
# STEP 1: EXPLORE SOURCE DATA
# ════════════════════════════════════════════════════════════
def step_explore(project_id):
    banner("STEP 1: Exploring Firebase Public Dataset")

    from src.ingestion.firebase_public_loader import FirebasePublicLoader
    loader = FirebasePublicLoader(project_id=project_id)

    results = loader.explore_source()

    if results.get("date_range") is not None:
        print(f"\n  {CHECK} Source dataset accessible")
        dr = results["date_range"]
        print(f"      Date range: {dr.iloc[0]['earliest_date']} to {dr.iloc[0]['latest_date']}")
        print(f"      Total events: {dr.iloc[0]['total_events']:,}")
        print(f"      Unique users: {dr.iloc[0]['unique_users']:,}")
    else:
        print(f"  {CROSS} Could not access source dataset")
        return False

    return True


# ════════════════════════════════════════════════════════════
# STEP 2: INGEST DATA
# ════════════════════════════════════════════════════════════
def step_ingest(project_id, date_from="2018-06-12", date_to="2018-10-03"):
    banner("STEP 2: Ingesting Data from Firebase Public Dataset")

    from src.ingestion.firebase_public_loader import FirebasePublicLoader
    loader = FirebasePublicLoader(project_id=project_id)

    # Create schemas
    print("  Creating raw schemas...")
    loader.create_raw_schemas()
    print(f"  {CHECK} Schemas created")

    # Ingest data
    print(f"  Ingesting {date_from} to {date_to}...")
    start = time.time()
    stats = loader.ingest_date_range(date_from, date_to, overwrite=True)
    duration = time.time() - start

    print(f"  {CHECK} Ingested {stats['rows_loaded']:,} rows in {duration:.1f}s")
    print(f"      Data processed: {stats['bytes_processed_gb']} GB")

    # Show stats
    loader.show_raw_stats()
    loader.show_event_breakdown()

    return True


# ════════════════════════════════════════════════════════════
# STEP 3: TRANSFORM DATA
# ════════════════════════════════════════════════════════════
def step_transform(project_id):
    banner("STEP 3: Running Data Transformations")

    from src.transformation.run_transforms import run_transforms

    start = time.time()
    results = run_transforms(project_id, layer="all")
    duration = time.time() - start

    # Summary
    success = sum(1 for v in results.values() if v.get("status") == "success")
    failed = sum(1 for v in results.values() if v.get("status") == "error")

    print(f"\n  {CHECK} {success} tables built successfully in {duration:.1f}s")
    if failed > 0:
        print(f"  {CROSS} {failed} tables failed")

    return failed == 0


# ════════════════════════════════════════════════════════════
# STEP 4: ML PIPELINE
# ════════════════════════════════════════════════════════════
def step_ml(project_id):
    banner("STEP 4: Running ML Pipeline")

    from google.cloud import bigquery
    from src.ml.feature_engineering import build_feature_store_sql
    from src.ml.churn_model import ChurnPredictor
    from src.ml.segmentation import PlayerSegmenter

    client = bigquery.Client(project="game-analytics-22")

    # Ensure ML dataset exists
    dataset_ref = f"{project_id}.game_ml"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)

    # Step 4a: Build feature store
    print("  Building feature store...")
    start = time.time()
    feature_sql = build_feature_store_sql(project_id)
    client.query(feature_sql).result()
    duration = time.time() - start

    # Check feature count
    count_query = f"SELECT COUNT(*) AS cnt FROM `{project_id}.game_ml.feature_store`"
    count_result = list(client.query(count_query).result())
    feature_count = count_result[0]["cnt"]
    print(f"  {CHECK} Feature store built: {feature_count:,} users ({duration:.1f}s)")

    # Step 4b: Train churn model
    print("  Training churn prediction model...")
    start = time.time()

    predictor = ChurnPredictor(project_id=project_id)
    metrics = predictor.train()
    duration = time.time() - start

    print(f"  {CHECK} Churn model trained ({duration:.1f}s)")
    print(f"      AUC-ROC: {metrics['auc_roc']:.4f}")
    print(f"      Precision: {metrics['precision']:.4f}")
    print(f"      Recall: {metrics['recall']:.4f}")
    print(f"      F1 Score: {metrics['f1_score']:.4f}")

    # Save model
    predictor.save_model()
    print(f"  {CHECK} Model saved")

    # Write predictions
    print("  Generating predictions...")
    predictor.predict_and_write()
    print(f"  {CHECK} Predictions written to BigQuery")

    # Step 4c: Run segmentation
    print("  Running player segmentation...")
    start = time.time()
    segmenter = PlayerSegmenter(project_id=project_id)
    seg_results = segmenter.run_segmentation()
    duration = time.time() - start

    print(f"  {CHECK} Segmentation complete ({duration:.1f}s)")
    for segment, count in seg_results["segment_counts"].items():
        pct = count / seg_results["total_users"] * 100
        print(f"      {segment}: {count:,} ({pct:.1f}%)")

    # Show feature importance
    print("\n  Top 10 Features for Churn Prediction:")
    importance = predictor.get_feature_importance(top_n=10)
    for _, row in importance.iterrows():
        bar = "#" * int(row["importance"] * 50)
        print(f"      {row['feature']:30s} {bar} ({row['importance']:.3f})")

    return True


# ════════════════════════════════════════════════════════════
# STEP 5: VERIFY PIPELINE
# ════════════════════════════════════════════════════════════
def step_verify(project_id):
    banner("STEP 5: Verifying Pipeline Output")

    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)

    checks = [
        ("Raw Events", f"""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT user_pseudo_id) AS users,
                   COUNT(DISTINCT event_name) AS events, MIN(event_date) AS start, MAX(event_date) AS end
            FROM `{project_id}.game_raw.raw_events`
        """),
        ("Staging Events", f"""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT user_pseudo_id) AS users
            FROM `{project_id}.game_staging.stg_events`
        """),
        ("Staging Sessions", f"""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT user_pseudo_id) AS users,
                   AVG(session_duration_sec) AS avg_duration
            FROM `{project_id}.game_staging.stg_sessions`
        """),
        ("Dim Users", f"""
            SELECT COUNT(*) AS users, COUNTIF(is_payer) AS payers,
                   AVG(active_days) AS avg_active_days, AVG(max_level) AS avg_level
            FROM `{project_id}.game_warehouse.dim_users`
        """),
        ("Fact Events", f"""
            SELECT COUNT(*) AS rows FROM `{project_id}.game_warehouse.fact_events`
        """),
        ("Fact Sessions", f"""
            SELECT COUNT(*) AS rows, AVG(session_duration_sec) AS avg_dur
            FROM `{project_id}.game_warehouse.fact_sessions`
        """),
        ("Fact Levels", f"""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT level_number) AS unique_levels
            FROM `{project_id}.game_warehouse.fact_levels`
        """),
        ("Daily KPIs", f"""
            SELECT COUNT(*) AS days, AVG(dau) AS avg_dau, AVG(stickiness) AS avg_stickiness
            FROM `{project_id}.game_marts.mart_daily_kpis`
        """),
        ("Retention Cohorts", f"""
            SELECT COUNT(*) AS cohorts, AVG(d1_retention) AS avg_d1, AVG(d7_retention) AS avg_d7
            FROM `{project_id}.game_marts.mart_retention_cohorts`
        """),
        ("Player Segments", f"""
            SELECT player_segment, COUNT(*) AS count
            FROM `{project_id}.game_marts.mart_player_segments`
            GROUP BY player_segment ORDER BY count DESC
        """),
        ("Level Funnel", f"""
            SELECT COUNT(*) AS levels, MIN(completion_rate) AS min_rate, MAX(completion_rate) AS max_rate
            FROM `{project_id}.game_marts.mart_level_funnel`
        """),
        ("ML Feature Store", f"""
            SELECT COUNT(*) AS users, AVG(is_churned) AS churn_rate
            FROM `{project_id}.game_ml.feature_store`
        """),
    ]

    all_ok = True
    for name, query in checks:
        try:
            df = client.query(query).to_dataframe()
            print(f"  {CHECK} {name}:")
            for _, row in df.iterrows():
                values = " | ".join(
                    f"{col}={row[col]:.2f}" if isinstance(row[col], float) else f"{col}={row[col]}"
                    for col in df.columns
                )
                print(f"      {values}")
        except Exception as e:
            print(f"  {CROSS} {name}: {e}")
            all_ok = False

    return all_ok


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Game Analytics Pipeline Runner")
    parser.add_argument("--project", default="game-analytics-22", help="GCP Project ID")
    parser.add_argument("--run", required=True,
                        choices=["all", "explore", "ingest", "transform", "ml", "verify"],
                        help="Pipeline step to run")
    parser.add_argument("--date-from", default="2018-06-12", help="Ingest start date")
    parser.add_argument("--date-to", default="2018-10-03", help="Ingest end date")

    args = parser.parse_args()

    banner(f"Game Analytics Pipeline - {args.run.upper()}")
    print(f"  Project: {args.project}")
    print(f"  Source: firebase-public-project.analytics_153293282 (Flood-It)")
    print()

    steps = {
        "explore": lambda: step_explore(args.project),
        "ingest": lambda: step_ingest(args.project, args.date_from, args.date_to),
        "transform": lambda: step_transform(args.project),
        "ml": lambda: step_ml(args.project),
        "verify": lambda: step_verify(args.project),
    }

    if args.run == "all":
        total_start = time.time()
        for step_name in ["explore", "ingest", "transform", "ml", "verify"]:
            success = steps[step_name]()
            if not success:
                print(f"\n{RED}Pipeline failed at step: {step_name}{RESET}")
                sys.exit(1)

        total_duration = time.time() - total_start
        banner(f"PIPELINE COMPLETE ({total_duration:.0f}s total)")
        print(f"  {GREEN}All steps completed successfully!{RESET}")
        print(f"\n  Next: Connect Google Data Studio to BigQuery datasets:")
        print(f"    - {args.project}.game_marts.mart_daily_kpis")
        print(f"    - {args.project}.game_marts.mart_retention_cohorts")
        print(f"    - {args.project}.game_marts.mart_player_segments")
        print(f"    - {args.project}.game_marts.mart_level_funnel")
        print()
    else:
        steps[args.run]()


if __name__ == "__main__":
    main()
