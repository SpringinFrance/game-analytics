"""
Game Analytics Pipeline - Configuration Settings
================================================
Central configuration for all pipeline modules.
Updated to use Firebase Gaming Public Dataset (Flood-It game)
from BigQuery Public Datasets as data source.
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class SourceConfig:
    """Firebase Public Dataset configuration (data source)."""
    # Public dataset: Flood-It mobile puzzle game
    source_project: str = "firebase-public-project"
    source_dataset: str = "analytics_153293282"
    # Table pattern: events_YYYYMMDD (2018-06-12 to 2018-10-03)
    table_pattern: str = "events_*"
    date_start: str = "2018-06-12"
    date_end: str = "2018-10-03"

    # GA4/Firebase event names relevant to gaming
    gaming_events: List[str] = field(default_factory=lambda: [
        "session_start",
        "user_engagement",
        "level_start",
        "level_up",
        "level_end",
        "level_retry",
        "level_reset",
        "level_complete",
        "post_score",
        "spend_virtual_currency",
        "earn_virtual_currency",
        "in_app_purchase",
        "ad_impression",
        "ad_click",
        "select_content",
        "app_remove",
    ])

    @property
    def events_table(self) -> str:
        return f"{self.source_project}.{self.source_dataset}.events_*"

    @property
    def events_table_prefix(self) -> str:
        return f"{self.source_project}.{self.source_dataset}.events_"


@dataclass
class BigQueryConfig:
    """BigQuery destination configuration."""
    project_id: str = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
    location: str = "US"

    # Dataset names (destination)
    dataset_raw: str = os.getenv("BIGQUERY_DATASET_RAW", "game_raw")
    dataset_staging: str = os.getenv("BIGQUERY_DATASET_STAGING", "game_staging")
    dataset_warehouse: str = os.getenv("BIGQUERY_DATASET_WAREHOUSE", "game_warehouse")
    dataset_marts: str = os.getenv("BIGQUERY_DATASET_MARTS", "game_marts")
    dataset_ml: str = os.getenv("BIGQUERY_DATASET_ML", "game_ml")

    # Raw table names
    raw_events: str = "raw_events"
    raw_sessions: str = "raw_sessions"
    raw_users: str = "raw_users"

    # Staging table names
    staging_events: str = "stg_events"
    staging_sessions: str = "stg_sessions"

    # Warehouse table names
    fact_events: str = "fact_events"
    fact_sessions: str = "fact_sessions"
    fact_revenue: str = "fact_revenue"
    fact_levels: str = "fact_levels"
    dim_users: str = "dim_users"
    dim_dates: str = "dim_dates"

    # Partition and retention
    raw_retention_days: int = 365  # Public data doesn't expire, keep all


@dataclass
class MLConfig:
    """Machine Learning pipeline configuration."""
    lookback_days: int = 30
    prediction_window_days: int = 7  # Churn = no activity in next 7 days

    # Model parameters
    model_type: str = "xgboost"
    test_size: float = 0.2
    random_state: int = 42

    xgb_params: dict = field(default_factory=lambda: {
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "scale_pos_weight": 2,
        "eval_metric": "logloss",
        "random_state": 42,
    })

    n_segments: int = 7
    rfm_quantiles: int = 4

    model_path: str = "models/"
    feature_table: str = "feature_store"
    prediction_table: str = "predictions"
    model_metadata_table: str = "model_metadata"

    min_precision: float = 0.75
    min_recall: float = 0.70
    min_auc: float = 0.85


@dataclass
class ScheduleConfig:
    """Pipeline scheduling (UTC)."""
    ingest_from_public: str = "0 6 * * *"
    staging_transform: str = "0 7 * * *"
    warehouse_build: str = "30 7 * * *"
    mart_refresh: str = "0 8 * * *"
    ml_feature_update: str = "30 8 * * *"
    ml_prediction: str = "0 9 * * *"
    ml_retrain: str = "0 2 * * 0"


# ── Singleton instances ─────────────────────────────────
source_config = SourceConfig()
bigquery_config = BigQueryConfig()
ml_config = MLConfig()
schedule_config = ScheduleConfig()
