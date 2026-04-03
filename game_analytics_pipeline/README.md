# Game Analytics Pipeline

End-to-end data analytics pipeline for mobile game data. Pulls event data from the **Firebase Public Dataset** (Flood-It game), transforms it through a multi-layer BigQuery warehouse, runs ML models for churn prediction and player segmentation, and serves dashboards via Looker Studio.

```
Firebase Public Dataset ──> BigQuery Raw ──> Staging ──> Warehouse ──> Marts ──> Looker Studio
                                                                  └──> ML (Churn + Segmentation)
```

## Data Source

| Property | Value |
|----------|-------|
| Source Project | `firebase-public-project` |
| Dataset | `analytics_153293282` |
| Game | Flood-It! (color puzzle game) |
| Date Range | 2018-06-12 to 2018-10-03 (114 days) |
| Events | 5,736,606 |
| Users | 15,175 |
| Schema | GA4/Firebase nested event schema |

## Tech Stack

- **Data Warehouse**: Google BigQuery (Sandbox / Free tier compatible)
- **Language**: Python 3.10+
- **Package Manager**: [uv](https://github.com/astral-sh/uv)
- **ML**: XGBoost, Scikit-learn
- **Visualization**: Looker Studio (Google Data Studio)
- **Infrastructure**: Google Cloud Platform (GCP)

## Quick Start

### Prerequisites

- Google Cloud account with a BigQuery project
- Python 3.10+
- [uv](https://docs.astral.sh/uv/) installed
- `gcloud` CLI authenticated (`gcloud auth application-default login`)

### Setup

```bash
cd game_analytics_pipeline

# Create virtual environment and install dependencies
uv venv
uv pip install -r requirements.txt
```

### Run the Pipeline

```bash
# Full pipeline (all 5 steps sequentially)
uv run python scripts/run_pipeline.py --project game-analytics-22 --run all

# Or run individual steps
uv run python scripts/run_pipeline.py --project game-analytics-22 --run explore    # Test source access
uv run python scripts/run_pipeline.py --project game-analytics-22 --run ingest     # Pull data from Firebase
uv run python scripts/run_pipeline.py --project game-analytics-22 --run transform  # Build 15 tables
uv run python scripts/run_pipeline.py --project game-analytics-22 --run ml         # Train models
uv run python scripts/run_pipeline.py --project game-analytics-22 --run verify     # Validate all tables
```

Replace `game-analytics-22` with your own GCP project ID.

## Pipeline Steps

### Step 1: Explore
Tests connectivity to the Firebase public dataset and prints date range, event counts, and user counts.

### Step 2: Ingest
Queries the Firebase public dataset using BigQuery cross-project access. Flattens the nested GA4 schema (RECORD structs via dot notation, REPEATED event_params via correlated UNNEST subqueries) into a single wide `raw_events` table with 30+ columns.

### Step 3: Transform
Builds 15 tables across 3 layers using pure BigQuery SQL (`CREATE OR REPLACE TABLE`):

| Layer | Tables | Purpose |
|-------|--------|---------|
| **Staging** (2) | `stg_events`, `stg_sessions` | Deduplicate events, construct sessions via 30-min gap logic |
| **Warehouse** (6) | `dim_dates`, `dim_users`, `fact_events`, `fact_sessions`, `fact_revenue`, `fact_levels` | Star schema: 2 dimensions + 4 fact tables |
| **Marts** (7) | `mart_daily_kpis`, `mart_retention_cohorts`, `mart_revenue_daily`, `mart_player_segments`, `mart_session_stats`, `mart_level_funnel`, `mart_geo_stats` | Pre-aggregated KPIs for dashboards |

### Step 4: ML
Runs three sub-steps:

1. **Feature Store** — Builds `game_ml.feature_store` with 42 features per user (activity, engagement, monetary, progression, derived ratios)
2. **Churn Prediction** — Trains XGBoost binary classifier on 15,175 users (churned = no session in 7 days). Outputs churn probability and risk tier per user
3. **Player Segmentation** — RFM (Recency, Frequency, Monetary) analysis with rule-based segment assignment:

| Segment | Description |
|---------|-------------|
| Whale | Top 2% spenders, high frequency |
| Dolphin | Regular spenders, moderate amounts |
| Minnow | Occasional small purchases |
| Engaged Free | Active non-payers |
| Casual | Low frequency, low engagement |
| At Risk | Declining activity, was active |
| Churned | No activity for 7+ days |

### Step 5: Verify
Runs health check queries across all tables, printing row counts and key metrics.

## Project Structure

```
game_analytics_pipeline/
├── scripts/
│   ├── run_pipeline.py              # Main orchestrator (--run [explore|ingest|transform|ml|verify|all])
│   ├── setup_gcp.py                 # GCP project setup helper
│   └── check_schema.py              # Schema inspection utility
├── src/
│   ├── ingestion/
│   │   ├── firebase_public_loader.py  # Core: Firebase extraction, schema flattening, BQ loading
│   │   ├── appsflyer_client.py        # Legacy: AppsFlyer Pull API client
│   │   └── bigquery_loader.py         # Legacy: Generic BQ loader
│   ├── transformation/
│   │   ├── sql_transforms.py          # All 15 SQL CREATE OR REPLACE TABLE statements
│   │   └── run_transforms.py          # Executor: runs SQL in dependency order
│   ├── ml/
│   │   ├── feature_engineering.py     # Feature store SQL builder (42 features)
│   │   ├── churn_model.py             # XGBoost churn prediction (train/predict/write)
│   │   └── segmentation.py            # RFM player segmentation
│   └── dashboards/
│       └── dashboard_queries.sql      # Looker Studio SQL queries (4 dashboards, 13 queries)
├── config/
│   └── settings.py                    # Source config, dataset names, date ranges
├── deploy/
│   ├── cloud_function_main.py         # Cloud Function entry point (pending Firebase update)
│   └── setup_scheduler.sh             # Cloud Scheduler setup script
├── models/                            # Trained model artifacts (.joblib)
├── .env.example                       # Environment variable template
├── .gitignore
└── requirements.txt                   # Python dependencies
```

## BigQuery Datasets

After running the full pipeline, these datasets are created:

| Dataset | Tables | Description |
|---------|--------|-------------|
| `game_raw` | 1 | Flattened Firebase events (source of truth) |
| `game_staging` | 2 | Cleaned, deduplicated events + sessions |
| `game_warehouse` | 6 | Star schema (dims + facts) |
| `game_marts` | 7 | Pre-aggregated KPIs for dashboards |
| `game_ml` | 4 | Feature store, predictions, segments, model metadata |

## BigQuery Sandbox Notes

This pipeline is fully compatible with the BigQuery free tier (sandbox). Key constraints and how they are handled:

| Limitation | Solution |
|------------|----------|
| DML blocked (INSERT/UPDATE/DELETE) | All writes use `CREATE OR REPLACE TABLE ... AS SELECT` |
| Partitioned tables write 0 rows | No `PARTITION BY` or `CLUSTER BY` on any table |
| `job_config.destination` unreliable | Inline `CREATE OR REPLACE TABLE` for cross-project queries |
| `table.num_rows` metadata lag | `SELECT COUNT(*)` for row count verification |
| 1 TB/month query limit | Column selection optimized, avoid `SELECT *` |

## Dashboards (Looker Studio)

Four dashboards are defined in `src/dashboards/dashboard_queries.sql`:

1. **Product Overview** — DAU/MAU trend, retention heatmap, session metrics, level funnel, geo map
2. **Revenue & Monetization** — Revenue by type, ARPU/ARPPU, player segments, LTV cohort curves
3. **Gaming Engagement** — Level difficulty, win rate by level, score distribution, traffic source performance
4. **Churn Prediction** — Risk tier distribution, churn by segment, model metrics

To connect: Open [Looker Studio](https://lookerstudio.google.com) > Add data source > BigQuery > select your project > choose mart tables.

## License

This project uses the [Firebase Public Dataset](https://console.cloud.google.com/marketplace/product/firebase-public/analytics) which is publicly available for educational and analytical purposes.
