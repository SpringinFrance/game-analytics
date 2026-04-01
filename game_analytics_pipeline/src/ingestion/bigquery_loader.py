"""
BigQuery Data Loader
====================
Handles loading data from AppsFlyer (via Pull API or Data Locker)
into BigQuery raw tables. Manages schema creation, deduplication,
and partition management.

Usage:
    loader = BigQueryLoader(project_id="your-project", dataset="game_raw")
    loader.create_schemas()
    loader.load_installs(df)
    loader.load_events(df)
"""

import logging
from datetime import datetime
from typing import Optional, List

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, LoadJobConfig, WriteDisposition

logger = logging.getLogger(__name__)


# ── Schema Definitions ──────────────────────────────────────
RAW_INSTALLS_SCHEMA = [
    SchemaField("attributed_touch_type", "STRING"),
    SchemaField("attributed_touch_time", "TIMESTAMP"),
    SchemaField("install_time", "TIMESTAMP"),
    SchemaField("event_time", "TIMESTAMP"),
    SchemaField("event_name", "STRING"),
    SchemaField("media_source", "STRING"),
    SchemaField("channel", "STRING"),
    SchemaField("campaign", "STRING"),
    SchemaField("campaign_id", "STRING"),
    SchemaField("adset", "STRING"),
    SchemaField("adset_id", "STRING"),
    SchemaField("ad", "STRING"),
    SchemaField("ad_id", "STRING"),
    SchemaField("site_id", "STRING"),
    SchemaField("sub_site_id", "STRING"),
    SchemaField("cost_model", "STRING"),
    SchemaField("cost_value", "FLOAT64"),
    SchemaField("cost_currency", "STRING"),
    SchemaField("appsflyer_id", "STRING"),
    SchemaField("customer_user_id", "STRING"),
    SchemaField("platform", "STRING"),
    SchemaField("device_type", "STRING"),
    SchemaField("os_version", "STRING"),
    SchemaField("app_version", "STRING"),
    SchemaField("country_code", "STRING"),
    SchemaField("city", "STRING"),
    SchemaField("ip", "STRING"),
    SchemaField("wifi", "BOOLEAN"),
    SchemaField("language", "STRING"),
    SchemaField("operator", "STRING"),
    SchemaField("carrier", "STRING"),
    SchemaField("_ingestion_timestamp", "TIMESTAMP"),
    SchemaField("_source", "STRING"),  # 'pull_api' or 'data_locker'
]

RAW_EVENTS_SCHEMA = [
    SchemaField("event_time", "TIMESTAMP"),
    SchemaField("event_name", "STRING"),
    SchemaField("event_value", "STRING"),  # JSON string with event parameters
    SchemaField("event_revenue", "FLOAT64"),
    SchemaField("event_revenue_currency", "STRING"),
    SchemaField("media_source", "STRING"),
    SchemaField("channel", "STRING"),
    SchemaField("campaign", "STRING"),
    SchemaField("campaign_id", "STRING"),
    SchemaField("adset", "STRING"),
    SchemaField("adset_id", "STRING"),
    SchemaField("appsflyer_id", "STRING"),
    SchemaField("customer_user_id", "STRING"),
    SchemaField("platform", "STRING"),
    SchemaField("device_type", "STRING"),
    SchemaField("os_version", "STRING"),
    SchemaField("app_version", "STRING"),
    SchemaField("country_code", "STRING"),
    SchemaField("city", "STRING"),
    SchemaField("ip", "STRING"),
    SchemaField("install_time", "TIMESTAMP"),
    SchemaField("_ingestion_timestamp", "TIMESTAMP"),
    SchemaField("_source", "STRING"),
]

RAW_UNINSTALLS_SCHEMA = [
    SchemaField("event_time", "TIMESTAMP"),
    SchemaField("appsflyer_id", "STRING"),
    SchemaField("customer_user_id", "STRING"),
    SchemaField("platform", "STRING"),
    SchemaField("media_source", "STRING"),
    SchemaField("campaign", "STRING"),
    SchemaField("country_code", "STRING"),
    SchemaField("install_time", "TIMESTAMP"),
    SchemaField("_ingestion_timestamp", "TIMESTAMP"),
    SchemaField("_source", "STRING"),
]


class BigQueryLoader:
    """Manages BigQuery schema creation and data loading."""

    def __init__(self, project_id: str, dataset: str, location: str = "US"):
        self.project_id = project_id
        self.dataset = dataset
        self.location = location
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = f"{project_id}.{dataset}"

    # ── Schema Management ───────────────────────────────────
    def create_dataset_if_not_exists(self):
        """Create the BigQuery dataset if it doesn't exist."""
        dataset = bigquery.Dataset(self.dataset_ref)
        dataset.location = self.location
        dataset.description = "Game Analytics - Raw data from AppsFlyer"

        try:
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {self.dataset_ref} ready")
        except Exception as e:
            logger.error(f"Failed to create dataset: {e}")
            raise

    def create_schemas(self):
        """Create all raw tables with proper partitioning and clustering."""
        self.create_dataset_if_not_exists()

        tables = [
            ("raw_installs", RAW_INSTALLS_SCHEMA, "install_time", ["platform", "country_code", "media_source"]),
            ("raw_events", RAW_EVENTS_SCHEMA, "event_time", ["event_name", "platform", "country_code"]),
            ("raw_uninstalls", RAW_UNINSTALLS_SCHEMA, "event_time", ["platform", "country_code"]),
        ]

        for table_name, schema, partition_field, cluster_fields in tables:
            table_ref = f"{self.dataset_ref}.{table_name}"
            table = bigquery.Table(table_ref, schema=schema)

            # Time partitioning for efficient querying and cost control
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
                expiration_ms=90 * 24 * 60 * 60 * 1000,  # 90-day retention
            )

            # Clustering for common query patterns
            table.clustering_fields = cluster_fields

            try:
                self.client.create_table(table, exists_ok=True)
                logger.info(f"Table {table_ref} ready")
            except Exception as e:
                logger.error(f"Failed to create table {table_ref}: {e}")
                raise

    # ── Data Loading ────────────────────────────────────────
    def load_installs(
        self,
        df: pd.DataFrame,
        source: str = "pull_api",
        write_disposition: str = "WRITE_APPEND",
    ) -> int:
        """
        Load install data into raw_installs table.

        Args:
            df: DataFrame with install data from AppsFlyer
            source: Data source identifier ('pull_api' or 'data_locker')
            write_disposition: BigQuery write mode

        Returns:
            Number of rows loaded
        """
        if df.empty:
            logger.warning("Empty DataFrame, skipping load")
            return 0

        df = self._prepare_dataframe(df, source)
        return self._load_to_table("raw_installs", df, write_disposition)

    def load_events(
        self,
        df: pd.DataFrame,
        source: str = "pull_api",
        write_disposition: str = "WRITE_APPEND",
    ) -> int:
        """Load in-app event data into raw_events table."""
        if df.empty:
            return 0

        df = self._prepare_dataframe(df, source)
        return self._load_to_table("raw_events", df, write_disposition)

    def load_uninstalls(
        self,
        df: pd.DataFrame,
        source: str = "pull_api",
        write_disposition: str = "WRITE_APPEND",
    ) -> int:
        """Load uninstall data into raw_uninstalls table."""
        if df.empty:
            return 0

        df = self._prepare_dataframe(df, source)
        return self._load_to_table("raw_uninstalls", df, write_disposition)

    # ── Deduplication ───────────────────────────────────────
    def deduplicate_table(self, table_name: str, partition_date: str):
        """
        Remove duplicate rows from a partitioned table for a specific date.
        Uses appsflyer_id + event_time + event_name as dedup key.
        """
        table_ref = f"{self.dataset_ref}.{table_name}"

        if table_name == "raw_installs":
            dedup_key = "appsflyer_id, install_time"
            partition_field = "install_time"
        elif table_name == "raw_events":
            dedup_key = "appsflyer_id, event_time, event_name"
            partition_field = "event_time"
        else:
            dedup_key = "appsflyer_id, event_time"
            partition_field = "event_time"

        query = f"""
        CREATE OR REPLACE TABLE `{table_ref}`
        PARTITION BY DATE({partition_field})
        AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {dedup_key}
                    ORDER BY _ingestion_timestamp DESC
                ) AS row_num
            FROM `{table_ref}`
            WHERE DATE({partition_field}) = '{partition_date}'
        )
        WHERE row_num = 1
        """

        logger.info(f"Deduplicating {table_ref} for {partition_date}")
        job = self.client.query(query)
        job.result()
        logger.info(f"Deduplication complete for {table_ref}")

    # ── Helper Methods ──────────────────────────────────────
    def _prepare_dataframe(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Add metadata columns and clean data."""
        df = df.copy()

        # Add ingestion metadata
        df["_ingestion_timestamp"] = datetime.utcnow()
        df["_source"] = source

        # Standardize column names (AppsFlyer uses spaces sometimes)
        df.columns = [
            col.strip().lower().replace(" ", "_").replace("-", "_")
            for col in df.columns
        ]

        # Parse timestamp columns
        timestamp_cols = [
            "event_time", "install_time", "attributed_touch_time",
        ]
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        # Parse numeric columns
        numeric_cols = ["event_revenue", "cost_value"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _load_to_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        write_disposition: str,
    ) -> int:
        """Load DataFrame into BigQuery table."""
        table_ref = f"{self.dataset_ref}.{table_name}"

        job_config = LoadJobConfig(
            write_disposition=getattr(WriteDisposition, write_disposition),
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
        )

        try:
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()  # Wait for completion

            logger.info(f"Loaded {len(df)} rows into {table_ref}")
            return len(df)

        except Exception as e:
            logger.error(f"Failed to load data into {table_ref}: {e}")
            raise


def run_daily_ingestion(
    api_token: str,
    app_id: str,
    project_id: str,
    dataset: str,
    target_date: Optional[str] = None,
):
    """
    Main entry point for daily data ingestion.
    Pulls all report types from AppsFlyer and loads into BigQuery.

    This function is designed to be called by a Cloud Function.
    """
    from .appsflyer_client import AppsFlyerClient

    client = AppsFlyerClient(api_token=api_token, app_id=app_id)
    loader = BigQueryLoader(project_id=project_id, dataset=dataset)

    # Ensure schemas exist
    loader.create_schemas()

    if target_date is None:
        from datetime import timedelta
        target_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    results = {}

    # Pull and load installs (paid + organic)
    for report_type, load_method in [
        ("installs", loader.load_installs),
        ("organic_installs", loader.load_installs),
        ("in_app_events", loader.load_events),
        ("organic_in_app_events", loader.load_events),
        ("uninstalls", loader.load_uninstalls),
    ]:
        try:
            logger.info(f"Pulling {report_type} for {target_date}")
            df = client.pull_daily_report(report_type, target_date)
            rows = load_method(df, source="pull_api")
            results[report_type] = {"status": "success", "rows": rows}
        except Exception as e:
            logger.error(f"Failed to process {report_type}: {e}")
            results[report_type] = {"status": "error", "error": str(e)}

    # Deduplicate after loading
    for table in ["raw_installs", "raw_events", "raw_uninstalls"]:
        try:
            loader.deduplicate_table(table, target_date)
        except Exception as e:
            logger.warning(f"Deduplication failed for {table}: {e}")

    logger.info(f"Daily ingestion complete: {results}")
    return results
