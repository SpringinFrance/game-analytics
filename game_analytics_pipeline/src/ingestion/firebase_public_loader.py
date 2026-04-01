"""
Firebase Public Dataset Loader
==============================
Pulls gaming analytics data from the BigQuery Public Dataset
(firebase-public-project.analytics_153293282 - Flood-It game)
and loads it into your project's raw tables.

The source uses GA4/Firebase event schema with nested fields:
  - event_params (REPEATED RECORD)
  - user_properties (REPEATED RECORD)
  - device (RECORD)
  - geo (RECORD)
  - traffic_source (RECORD)

This module flattens the nested structure into analysis-ready tables.

Usage:
    loader = FirebasePublicLoader(project_id="your-project")
    loader.create_raw_schemas()
    stats = loader.ingest_date_range("2018-07-01", "2018-07-31")
    loader.show_stats()
"""

import logging
from datetime import datetime
from typing import Optional, Dict, List

from google.cloud import bigquery

logger = logging.getLogger(__name__)


# ── Source dataset info ─────────────────────────────────
SOURCE_PROJECT = "firebase-public-project"
SOURCE_DATASET = "analytics_153293282"
SOURCE_TABLE = f"{SOURCE_PROJECT}.{SOURCE_DATASET}.events_*"


class FirebasePublicLoader:
    """
    Loads and flattens Firebase gaming public dataset into local BigQuery tables.
    """

    def __init__(
        self,
        project_id: str,
        raw_dataset: str = "game_raw",
        location: str = "US",
    ):
        self.project_id = project_id
        self.raw_dataset = raw_dataset
        self.location = location
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = f"{project_id}.{raw_dataset}"

    # ════════════════════════════════════════════════════
    # SCHEMA CREATION
    # ════════════════════════════════════════════════════

    def create_raw_schemas(self):
        """Create destination dataset and raw tables."""
        # Create dataset
        dataset = bigquery.Dataset(self.dataset_ref)
        dataset.location = self.location
        dataset.description = "Game Analytics - Raw data from Firebase Public Dataset (Flood-It)"
        self.client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset {self.dataset_ref} ready")

        # Create raw_events table
        self._create_raw_events_table()
        logger.info("All raw schemas created")

    def _create_raw_events_table(self):
        """Create the flattened raw events table."""
        table_ref = f"{self.dataset_ref}.raw_events"

        schema = [
            # ── Event core ──
            bigquery.SchemaField("event_date", "DATE"),
            bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("event_name", "STRING"),
            bigquery.SchemaField("event_value_in_usd", "FLOAT64"),

            # ── User ──
            bigquery.SchemaField("user_pseudo_id", "STRING"),
            bigquery.SchemaField("user_id", "STRING"),
            bigquery.SchemaField("user_first_touch_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("is_active_user", "BOOLEAN"),

            # ── Platform ──
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("stream_id", "STRING"),

            # ── Device (flattened) ──
            bigquery.SchemaField("device_category", "STRING"),
            bigquery.SchemaField("device_brand", "STRING"),
            bigquery.SchemaField("device_model", "STRING"),
            bigquery.SchemaField("device_os", "STRING"),
            bigquery.SchemaField("device_os_version", "STRING"),
            bigquery.SchemaField("device_language", "STRING"),

            # ── Geo (flattened) ──
            bigquery.SchemaField("geo_continent", "STRING"),
            bigquery.SchemaField("geo_country", "STRING"),
            bigquery.SchemaField("geo_region", "STRING"),
            bigquery.SchemaField("geo_city", "STRING"),

            # ── Traffic Source (flattened) ──
            bigquery.SchemaField("traffic_source", "STRING"),
            bigquery.SchemaField("traffic_medium", "STRING"),
            bigquery.SchemaField("traffic_campaign", "STRING"),

            # ── Flattened event_params (gaming-specific) ──
            bigquery.SchemaField("param_level", "INT64"),
            bigquery.SchemaField("param_score", "INT64"),
            bigquery.SchemaField("param_board", "STRING"),
            bigquery.SchemaField("param_success", "STRING"),
            bigquery.SchemaField("param_value", "FLOAT64"),
            bigquery.SchemaField("param_virtual_currency_name", "STRING"),
            bigquery.SchemaField("param_engagement_time_msec", "INT64"),
            bigquery.SchemaField("param_firebase_screen", "STRING"),
            bigquery.SchemaField("param_firebase_screen_class", "STRING"),
            bigquery.SchemaField("param_session_id", "STRING"),

            # ── Metadata ──
            bigquery.SchemaField("_ingestion_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("_source_date_suffix", "STRING"),
        ]

        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="event_date",
        )
        table.clustering_fields = ["event_name", "platform", "geo_country"]

        self.client.create_table(table, exists_ok=True)
        logger.info(f"Table {table_ref} ready")

    # ════════════════════════════════════════════════════
    # DATA INGESTION
    # ════════════════════════════════════════════════════

    def ingest_date_range(
        self,
        date_from: str,
        date_to: str,
        overwrite: bool = False,
    ) -> Dict:
        """
        Pull data from Firebase public dataset and load into raw tables.

        Args:
            date_from: Start date (YYYY-MM-DD), earliest: 2018-06-12
            date_to: End date (YYYY-MM-DD), latest: 2018-10-03
            overwrite: If True, replace existing data for the date range

        Returns:
            Dict with ingestion stats
        """
        # Convert dates to BigQuery table suffix format
        suffix_from = date_from.replace("-", "")
        suffix_to = date_to.replace("-", "")

        logger.info(f"Ingesting Firebase data from {date_from} to {date_to}")

        # Build the extraction + flattening query
        query = self._build_extraction_query(suffix_from, suffix_to)

        # Configure destination
        dest_table = f"{self.dataset_ref}.raw_events"

        job_config = bigquery.QueryJobConfig(
            destination=dest_table,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE if overwrite
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
            time_partitioning=bigquery.TimePartitioning(
                field="event_date",
                type_=bigquery.TimePartitioningType.DAY,
            ),
            clustering_fields=["event_name", "platform", "geo_country"]
        )

        logger.info("Running extraction query...")
        job = self.client.query(query, job_config=job_config)
        result = job.result()

        rows = job.output_rows
        bytes_processed = job.total_bytes_processed or 0

        stats = {
            "rows_loaded": rows,
            "bytes_processed": bytes_processed,
            "bytes_processed_gb": round(bytes_processed / (1024**3), 3),
            "date_from": date_from,
            "date_to": date_to,
            "destination": dest_table,
        }

        logger.info(
            f"Loaded {rows:,} rows ({stats['bytes_processed_gb']} GB processed) "
            f"into {dest_table}"
        )
        return stats

    def ingest_all(self, overwrite: bool = True) -> Dict:
        """Ingest the entire available date range."""
        return self.ingest_date_range(
            date_from="2018-06-12",
            date_to="2018-10-03",
            overwrite=overwrite,
        )

    def _build_extraction_query(self, suffix_from: str, suffix_to: str) -> str:
        """
        Build SQL that reads from the public dataset, flattens nested fields,
        and extracts gaming-specific event_params.
        """
        return f"""
        SELECT
            -- ── Event Core ──
            PARSE_DATE('%Y%m%d', event_date) AS event_date,
            TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
            event_name,
            event_value_in_usd,

            -- ── User ──
            user_pseudo_id,
            user_id,
            TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
            is_active_user,

            -- ── Platform ──
            platform,
            stream_id,

            -- ── Device (flatten RECORD) ──
            device.category AS device_category,
            device.mobile_brand_name AS device_brand,
            device.mobile_model_name AS device_model,
            device.operating_system AS device_os,
            device.operating_system_version AS device_os_version,
            device.language AS device_language,

            -- ── Geo (flatten RECORD) ──
            geo.continent AS geo_continent,
            geo.country AS geo_country,
            geo.region AS geo_region,
            geo.city AS geo_city,

            -- ── Traffic Source (flatten RECORD) ──
            traffic_source.source AS traffic_source,
            traffic_source.medium AS traffic_medium,
            traffic_source.name AS traffic_campaign,

            -- ── Event Params (flatten REPEATED via subqueries) ──
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

            -- ── Metadata ──
            CURRENT_TIMESTAMP() AS _ingestion_timestamp,
            _TABLE_SUFFIX AS _source_date_suffix,

        FROM `{SOURCE_PROJECT}.{SOURCE_DATASET}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{suffix_from}' AND '{suffix_to}'
        """

    # ════════════════════════════════════════════════════
    # EXPLORATION & STATS
    # ════════════════════════════════════════════════════

    def explore_source(self) -> Dict:
        """
        Explore the source public dataset structure.
        Returns stats about available data without loading anything.
        """
        logger.info("Exploring Firebase public dataset...")

        queries = {
            "date_range": f"""
                SELECT
                    MIN(PARSE_DATE('%Y%m%d', event_date)) AS earliest_date,
                    MAX(PARSE_DATE('%Y%m%d', event_date)) AS latest_date,
                    COUNT(*) AS total_events,
                    COUNT(DISTINCT user_pseudo_id) AS unique_users,
                FROM `{SOURCE_TABLE}`
            """,
            "event_names": f"""
                SELECT
                    event_name,
                    COUNT(*) AS event_count,
                    COUNT(DISTINCT user_pseudo_id) AS unique_users,
                    ROUND(AVG(event_value_in_usd), 4) AS avg_value_usd,
                FROM `{SOURCE_TABLE}`
                GROUP BY event_name
                ORDER BY event_count DESC
            """,
            "platforms": f"""
                SELECT
                    platform,
                    COUNT(*) AS events,
                    COUNT(DISTINCT user_pseudo_id) AS users,
                FROM `{SOURCE_TABLE}`
                GROUP BY platform
            """,
            "top_countries": f"""
                SELECT
                    geo.country AS country,
                    COUNT(*) AS events,
                    COUNT(DISTINCT user_pseudo_id) AS users,
                FROM `{SOURCE_TABLE}`
                GROUP BY country
                ORDER BY users DESC
                LIMIT 15
            """,
            "event_params_keys": f"""
                SELECT
                    ep.key,
                    COUNT(*) AS occurrences,
                FROM `{SOURCE_TABLE}`, UNNEST(event_params) AS ep
                GROUP BY ep.key
                ORDER BY occurrences DESC
                LIMIT 30
            """,
        }

        results = {}
        for name, query in queries.items():
            try:
                df = self.client.query(query).to_dataframe()
                results[name] = df
                logger.info(f"\n{'─' * 50}\n{name.upper()}:\n{df.to_string(index=False)}")
            except Exception as e:
                logger.error(f"Failed to run {name}: {e}")
                results[name] = None

        return results

    def show_raw_stats(self):
        """Show stats about data already loaded in raw tables."""
        query = f"""
        SELECT
            'raw_events' AS table_name,
            COUNT(*) AS total_rows,
            COUNT(DISTINCT user_pseudo_id) AS unique_users,
            COUNT(DISTINCT event_name) AS unique_events,
            MIN(event_date) AS earliest_date,
            MAX(event_date) AS latest_date,
            COUNT(DISTINCT event_date) AS days_of_data,
        FROM `{self.dataset_ref}.raw_events`
        """

        try:
            df = self.client.query(query).to_dataframe()
            logger.info(f"\nRaw Table Stats:\n{df.to_string(index=False)}")
            return df
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return None

    def show_event_breakdown(self):
        """Show event name distribution in raw data."""
        query = f"""
        SELECT
            event_name,
            COUNT(*) AS count,
            COUNT(DISTINCT user_pseudo_id) AS unique_users,
            ROUND(SUM(COALESCE(event_value_in_usd, 0)), 2) AS total_value_usd,
            AVG(param_engagement_time_msec) / 1000 AS avg_engagement_sec,
        FROM `{self.dataset_ref}.raw_events`
        GROUP BY event_name
        ORDER BY count DESC
        """

        try:
            df = self.client.query(query).to_dataframe()
            logger.info(f"\nEvent Breakdown:\n{df.to_string(index=False)}")
            return df
        except Exception as e:
            logger.error(f"Failed: {e}")
            return None
