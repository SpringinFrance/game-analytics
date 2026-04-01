"""
Cloud Function Entry Points
============================
Deploy these functions to Google Cloud Functions for automated pipeline execution.
Each function is triggered by Cloud Scheduler or GCS events.

Deployment:
    gcloud functions deploy daily_ingestion \
        --runtime python311 \
        --trigger-http \
        --entry-point daily_ingestion \
        --set-env-vars APPSFLYER_API_TOKEN=xxx,APPSFLYER_APP_ID=xxx,GCP_PROJECT_ID=xxx
"""

import os
import logging
import json
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Environment Config ──────────────────────────────────────
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project")
API_TOKEN = os.environ.get("APPSFLYER_API_TOKEN", "")
APP_ID = os.environ.get("APPSFLYER_APP_ID", "")
DATASET_RAW = os.environ.get("BIGQUERY_DATASET_RAW", "game_raw")


def daily_ingestion(request):
    """
    HTTP-triggered Cloud Function for daily AppsFlyer data ingestion.
    Triggered by Cloud Scheduler at 06:00 UTC daily.
    """
    from src.ingestion.bigquery_loader import run_daily_ingestion

    # Allow date override via request body
    target_date = None
    if request and request.get_json():
        target_date = request.get_json().get("date")

    try:
        results = run_daily_ingestion(
            api_token=API_TOKEN,
            app_id=APP_ID,
            project_id=PROJECT_ID,
            dataset=DATASET_RAW,
            target_date=target_date,
        )
        return json.dumps({"status": "success", "results": results}), 200

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return json.dumps({"status": "error", "error": str(e)}), 500


def run_transformations(request):
    """
    HTTP-triggered Cloud Function for running SQL transformations.
    Triggered by Cloud Scheduler at 07:00 UTC daily.
    """
    from src.transformation.run_transforms import run_transforms

    # Allow layer override
    layer = "all"
    if request and request.get_json():
        layer = request.get_json().get("layer", "all")

    try:
        results = run_transforms(PROJECT_ID, layer=layer)
        return json.dumps({"status": "success", "results": str(results)}), 200

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        return json.dumps({"status": "error", "error": str(e)}), 500


def run_ml_pipeline(request):
    """
    HTTP-triggered Cloud Function for ML pipeline.
    Triggered by Cloud Scheduler at 08:30 UTC daily (features + predictions).
    Weekly at 02:00 UTC Sunday (full retrain).
    """
    from google.cloud import bigquery
    from src.ml.feature_engineering import build_feature_store_sql
    from src.ml.churn_model import ChurnPredictor
    from src.ml.segmentation import PlayerSegmenter

    mode = "predict"  # default: daily prediction
    if request and request.get_json():
        mode = request.get_json().get("mode", "predict")

    try:
        client = bigquery.Client(project=PROJECT_ID)

        # Step 1: Update feature store
        logger.info("Updating feature store...")
        feature_sql = build_feature_store_sql(PROJECT_ID)
        client.query(feature_sql).result()
        logger.info("Feature store updated")

        # Step 2: Churn prediction (or retrain)
        predictor = ChurnPredictor(project_id=PROJECT_ID)

        if mode == "retrain":
            logger.info("Retraining churn model...")
            metrics = predictor.train()
            predictor.save_model()
            predictor.predict_and_write()
            result = {"mode": "retrain", "metrics": metrics}
        else:
            # Load existing model and predict
            import glob
            model_files = sorted(glob.glob("models/churn_model_*.joblib"))
            if model_files:
                predictor.load_model(model_files[-1])
                predictor.predict_and_write()
                result = {"mode": "predict", "status": "predictions_written"}
            else:
                # No model exists, train first
                logger.info("No existing model found, training...")
                metrics = predictor.train()
                predictor.save_model()
                predictor.predict_and_write()
                result = {"mode": "initial_train", "metrics": metrics}

        # Step 3: Run segmentation
        logger.info("Running player segmentation...")
        segmenter = PlayerSegmenter(project_id=PROJECT_ID)
        seg_results = segmenter.run_segmentation()
        result["segmentation"] = seg_results["segment_counts"]

        return json.dumps({"status": "success", "results": str(result)}), 200

    except Exception as e:
        logger.error(f"ML pipeline failed: {e}")
        return json.dumps({"status": "error", "error": str(e)}), 500


def data_locker_trigger(event, context):
    """
    GCS-triggered Cloud Function for Data Locker files.
    Fires when new files arrive in the Data Locker bucket.
    """
    from src.ingestion.appsflyer_client import AppsFlyerDataLocker
    from src.ingestion.bigquery_loader import BigQueryLoader

    bucket = event["bucket"]
    file_path = event["name"]

    logger.info(f"Data Locker file received: gs://{bucket}/{file_path}")

    # Parse the file path to determine report type and date
    # Expected: data-locker-hourly/t={type}/dt={date}/h={hour}/part-*.parquet
    parts = file_path.split("/")
    report_type = None
    date = None

    for part in parts:
        if part.startswith("t="):
            report_type = part[2:]
        elif part.startswith("dt="):
            date = part[3:]

    if not report_type or not date:
        logger.warning(f"Could not parse path: {file_path}")
        return

    try:
        locker = AppsFlyerDataLocker(bucket_name=bucket, project_id=PROJECT_ID)
        df = locker.load_hourly_data(report_type, date)

        if df.empty:
            logger.info("No data in file")
            return

        loader = BigQueryLoader(project_id=PROJECT_ID, dataset=DATASET_RAW)

        if report_type in ("installs", "organic_installs"):
            loader.load_installs(df, source="data_locker")
        elif report_type in ("in_app_events", "organic_in_app_events"):
            loader.load_events(df, source="data_locker")
        elif report_type == "uninstalls":
            loader.load_uninstalls(df, source="data_locker")

        logger.info(f"Loaded {len(df)} rows from Data Locker ({report_type})")

    except Exception as e:
        logger.error(f"Data Locker load failed: {e}")
        raise
