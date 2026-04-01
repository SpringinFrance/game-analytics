"""
AppsFlyer Pull API Client
=========================
Handles authentication, data extraction, and error handling for the
AppsFlyer Pull API v5. Supports all report types relevant to game analytics.

Usage:
    client = AppsFlyerClient(api_token="your_token", app_id="com.game.id")
    df = client.pull_report("installs", date_from="2026-03-01", date_to="2026-03-31")
"""

import io
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import requests
import pandas as pd

logger = logging.getLogger(__name__)


class AppsFlyerClient:
    """Client for AppsFlyer Pull API v5."""

    BASE_URL = "https://hq1.appsflyer.com/api/raw-data/export/app"

    # Available report types
    REPORT_TYPES = {
        "installs": "installs_report/v5",
        "in_app_events": "in_app_events_report/v5",
        "uninstalls": "uninstall_events_report/v5",
        "organic_installs": "organic_installs_report/v5",
        "organic_in_app_events": "organic_in_app_events_report/v5",
    }

    def __init__(
        self,
        api_token: str,
        app_id: str,
        max_retries: int = 3,
        retry_delay: int = 60,
        timeout: int = 300,
    ):
        self.api_token = api_token
        self.app_id = app_id
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "text/csv",
        })

    def pull_report(
        self,
        report_type: str,
        date_from: str,
        date_to: str,
        media_source: Optional[str] = None,
        event_name: Optional[str] = None,
        additional_fields: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Pull a raw data report from AppsFlyer.

        Args:
            report_type: One of 'installs', 'in_app_events', 'uninstalls',
                        'organic_installs', 'organic_in_app_events'
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            media_source: Filter by media source (optional)
            event_name: Filter by event name, comma-separated (optional)
            additional_fields: Extra fields to include (optional)

        Returns:
            pandas DataFrame with the report data
        """
        if report_type not in self.REPORT_TYPES:
            raise ValueError(
                f"Invalid report_type: {report_type}. "
                f"Valid types: {list(self.REPORT_TYPES.keys())}"
            )

        endpoint = self.REPORT_TYPES[report_type]
        url = f"{self.BASE_URL}/{self.app_id}/{endpoint}"

        params: Dict[str, Any] = {
            "from": date_from,
            "to": date_to,
        }

        if media_source:
            params["media_source"] = media_source
        if event_name:
            params["event_name"] = event_name
        if additional_fields:
            params["additional_fields"] = additional_fields

        logger.info(
            f"Pulling {report_type} report for {self.app_id} "
            f"from {date_from} to {date_to}"
        )

        return self._request_with_retry(url, params)

    def pull_daily_report(
        self,
        report_type: str,
        target_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Pull data for a single day (defaults to yesterday).

        Args:
            report_type: Report type to pull
            target_date: Date to pull (YYYY-MM-DD), defaults to yesterday

        Returns:
            pandas DataFrame with the day's data
        """
        if target_date is None:
            yesterday = datetime.utcnow() - timedelta(days=1)
            target_date = yesterday.strftime("%Y-%m-%d")

        return self.pull_report(report_type, date_from=target_date, date_to=target_date)

    def pull_gaming_events(
        self,
        date_from: str,
        date_to: str,
        events: Optional[list] = None,
    ) -> pd.DataFrame:
        """
        Pull in-app events filtered to gaming-specific events.

        Args:
            date_from: Start date
            date_to: End date
            events: List of event names to filter (uses defaults if None)

        Returns:
            DataFrame with gaming events
        """
        if events is None:
            events = [
                "af_purchase",
                "af_level_achieved",
                "af_tutorial_completion",
                "af_spent_credits",
                "af_achievement_unlocked",
                "af_ad_view",
            ]

        event_str = ",".join(events)
        return self.pull_report(
            "in_app_events",
            date_from=date_from,
            date_to=date_to,
            event_name=event_str,
        )

    def _request_with_retry(self, url: str, params: Dict) -> pd.DataFrame:
        """Execute API request with exponential backoff retry."""
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url, params=params, timeout=self.timeout, stream=True
                )

                if response.status_code == 200:
                    content = response.content.decode("utf-8")
                    if not content.strip():
                        logger.warning("Empty response received, returning empty DataFrame")
                        return pd.DataFrame()

                    df = pd.read_csv(io.StringIO(content))
                    logger.info(f"Successfully pulled {len(df)} rows")
                    return df

                elif response.status_code == 429:
                    # Rate limited
                    wait_time = self.retry_delay * attempt
                    logger.warning(
                        f"Rate limited (429). Waiting {wait_time}s "
                        f"(attempt {attempt}/{self.max_retries})"
                    )
                    time.sleep(wait_time)

                elif response.status_code == 401:
                    raise PermissionError(
                        "Authentication failed. Check your API token. "
                        "Note: V2 tokens generated before March 2026 were revoked."
                    )

                elif response.status_code == 404:
                    raise ValueError(
                        f"App ID '{self.app_id}' not found or report not available."
                    )

                else:
                    logger.error(
                        f"API error {response.status_code}: {response.text[:500]}"
                    )
                    last_exception = Exception(
                        f"HTTP {response.status_code}: {response.text[:200]}"
                    )

            except requests.exceptions.Timeout:
                logger.warning(
                    f"Request timed out (attempt {attempt}/{self.max_retries})"
                )
                last_exception = TimeoutError("Request timed out")

            except requests.exceptions.ConnectionError as e:
                logger.warning(
                    f"Connection error (attempt {attempt}/{self.max_retries}): {e}"
                )
                last_exception = e

            if attempt < self.max_retries:
                wait_time = self.retry_delay * attempt
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)

        raise RuntimeError(
            f"Failed after {self.max_retries} attempts. Last error: {last_exception}"
        )


class AppsFlyerDataLocker:
    """
    Handler for AppsFlyer Data Locker files stored in GCS.
    Loads Parquet/CSV files from the Data Locker bucket into DataFrames.
    """

    def __init__(self, bucket_name: str, project_id: str):
        self.bucket_name = bucket_name
        self.project_id = project_id

    def load_hourly_data(
        self,
        report_type: str,
        date: str,
        hour: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load Data Locker files from GCS for a given date and optional hour.

        Args:
            report_type: 'installs', 'in_app_events', etc.
            date: Date string (YYYY-MM-DD)
            hour: Specific hour (0-23), or None for all hours

        Returns:
            DataFrame with the loaded data
        """
        from google.cloud import storage

        client = storage.Client(project=self.project_id)
        bucket = client.bucket(self.bucket_name)

        # Data Locker path structure
        prefix = f"data-locker-hourly/t={report_type}/dt={date}/"
        if hour is not None:
            prefix += f"h={hour}/"

        blobs = list(bucket.list_blobs(prefix=prefix))
        if not blobs:
            logger.warning(f"No Data Locker files found at {prefix}")
            return pd.DataFrame()

        dfs = []
        for blob in blobs:
            if blob.name.endswith(".parquet"):
                content = blob.download_as_bytes()
                df = pd.read_parquet(io.BytesIO(content))
                dfs.append(df)
            elif blob.name.endswith(".csv"):
                content = blob.download_as_string().decode("utf-8")
                df = pd.read_csv(io.StringIO(content))
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        result = pd.concat(dfs, ignore_index=True)
        logger.info(
            f"Loaded {len(result)} rows from Data Locker "
            f"({report_type}, {date}, hour={hour})"
        )
        return result
