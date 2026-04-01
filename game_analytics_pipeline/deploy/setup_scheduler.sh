#!/bin/bash
# ============================================================
# Cloud Scheduler Setup Script
# ============================================================
# Creates scheduled jobs for the game analytics pipeline.
# Run after deploying Cloud Functions.
#
# Usage: bash deploy/setup_scheduler.sh <PROJECT_ID> <REGION>
# Example: bash deploy/setup_scheduler.sh my-project us-central1
# ============================================================

set -e

PROJECT_ID=${1:-"your-project-id"}
REGION=${2:-"us-central1"}
BASE_URL="https://${REGION}-${PROJECT_ID}.cloudfunctions.net"

echo "Setting up Cloud Scheduler jobs for project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Base URL: ${BASE_URL}"
echo ""

# ── Daily Ingestion (06:00 UTC) ─────────────────────────
echo "Creating daily_ingestion job..."
gcloud scheduler jobs create http daily-ingestion \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --schedule="0 6 * * *" \
    --uri="${BASE_URL}/daily_ingestion" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{}' \
    --time-zone="UTC" \
    --description="Pull daily data from AppsFlyer and load to BigQuery" \
    --attempt-deadline="600s" \
    --max-retry-attempts=3 \
    --min-backoff-duration="60s" \
    || echo "Job already exists, updating..."

# ── Daily Transformations (07:00 UTC) ───────────────────
echo "Creating run_transformations job..."
gcloud scheduler jobs create http daily-transformations \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --schedule="0 7 * * *" \
    --uri="${BASE_URL}/run_transformations" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"layer": "all"}' \
    --time-zone="UTC" \
    --description="Run staging, warehouse, and mart transformations" \
    --attempt-deadline="1200s" \
    --max-retry-attempts=2 \
    || echo "Job already exists, updating..."

# ── Daily ML Predictions (08:30 UTC) ───────────────────
echo "Creating daily_ml_predictions job..."
gcloud scheduler jobs create http daily-ml-predictions \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --schedule="30 8 * * *" \
    --uri="${BASE_URL}/run_ml_pipeline" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"mode": "predict"}' \
    --time-zone="UTC" \
    --description="Update features, run churn predictions, update segments" \
    --attempt-deadline="900s" \
    --max-retry-attempts=2 \
    || echo "Job already exists, updating..."

# ── Weekly Model Retrain (Sunday 02:00 UTC) ─────────────
echo "Creating weekly_model_retrain job..."
gcloud scheduler jobs create http weekly-model-retrain \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --schedule="0 2 * * 0" \
    --uri="${BASE_URL}/run_ml_pipeline" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"mode": "retrain"}' \
    --time-zone="UTC" \
    --description="Weekly full retrain of churn prediction model" \
    --attempt-deadline="1800s" \
    --max-retry-attempts=1 \
    || echo "Job already exists, updating..."

echo ""
echo "All scheduler jobs created successfully!"
echo ""
echo "Verify with: gcloud scheduler jobs list --project=${PROJECT_ID} --location=${REGION}"
