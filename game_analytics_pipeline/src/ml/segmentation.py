"""
Player Segmentation Model
=========================
Classifies players into behavioral segments using RFM analysis
combined with engagement and spending patterns.

Segments:
    - Whale: Top 2% spenders, high frequency
    - Dolphin: Regular spenders, moderate amounts
    - Minnow: Occasional small purchases
    - Engaged Free: Active non-payers
    - Casual: Low engagement, low spend
    - At-Risk: Declining activity (was engaged)
    - Churned: No activity for 7+ days

Usage:
    segmenter = PlayerSegmenter(project_id="your-project")
    results = segmenter.run_segmentation()
"""

import logging
from datetime import datetime
from typing import Optional, Dict

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

logger = logging.getLogger(__name__)


class PlayerSegmenter:
    """Player segmentation using RFM + behavioral clustering."""

    # Segment definitions with spending thresholds
    SEGMENT_RULES = {
        "whale": {
            "description": "Top spenders, high frequency",
            "strategy": "VIP treatment, exclusive content, personal offers",
        },
        "dolphin": {
            "description": "Regular spenders, moderate amounts",
            "strategy": "Upsell opportunities, bundle offers, loyalty rewards",
        },
        "minnow": {
            "description": "Occasional small purchases",
            "strategy": "Conversion optimization, special first-purchase offers",
        },
        "engaged_free": {
            "description": "Active players, zero spend",
            "strategy": "Monetization nudges, rewarded ads, limited-time offers",
        },
        "casual": {
            "description": "Low frequency, low engagement",
            "strategy": "Re-engagement campaigns, simplify onboarding",
        },
        "at_risk": {
            "description": "Declining activity, previously active",
            "strategy": "Win-back offers, push notifications, email campaigns",
        },
        "churned": {
            "description": "No activity for 7+ days",
            "strategy": "Retargeting ads, comeback bonuses, survey for feedback",
        },
    }

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.scaler = StandardScaler()

    def load_user_metrics(self) -> pd.DataFrame:
        """Load user metrics from feature store."""
        from google.cloud import bigquery

        client = bigquery.Client(project=self.project_id)

        query = f"""
        SELECT
            user_pseudo_id,
            platform,
            country,
            account_age_days,
            is_payer,

            -- RFM components
            days_since_last_session AS recency,
            active_days AS frequency,
            total_spend AS monetary,

            -- Additional features for refined segmentation
            sessions_last_7d,
            sessions_last_14d,
            avg_session_duration,
            total_play_time,
            max_level,
            levels_completed,
            purchase_count,
            iap_count,
            avg_transaction_value,
            ad_views,
            event_trend_ratio,
            session_acceleration,
            activity_rate,
            level_velocity,

        FROM `{self.project_id}.game_ml.feature_store`
        WHERE feature_computed_at = (
            SELECT MAX(feature_computed_at)
            FROM `{self.project_id}.game_ml.feature_store`
        )
        """

        df = client.query(query).to_dataframe()
        logger.info(f"Loaded {len(df)} user records for segmentation")
        return df

    def compute_rfm_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute RFM scores (1-4 scale, 4 = best).
        Recency is inverted (lower days = higher score).
        """
        df = df.copy()

        # Recency: lower is better → invert for scoring
        df["r_score"] = pd.qcut(
            df["recency"].rank(method="first"),
            q=4, labels=[4, 3, 2, 1]  # Low recency = high score
        ).astype(int)

        # Frequency: higher is better
        df["f_score"] = pd.qcut(
            df["frequency"].rank(method="first"),
            q=4, labels=[1, 2, 3, 4]
        ).astype(int)

        # Monetary: higher is better
        # Handle zero-spend users separately
        payers = df[df["monetary"] > 0].copy()
        non_payers = df[df["monetary"] == 0].copy()

        if len(payers) > 0:
            payers["m_score"] = pd.qcut(
                payers["monetary"].rank(method="first"),
                q=4, labels=[1, 2, 3, 4]
            ).astype(int)
        else:
            payers["m_score"] = 0

        non_payers["m_score"] = 0

        df = pd.concat([payers, non_payers], ignore_index=True)
        df["rfm_total"] = df["r_score"] + df["f_score"] + df["m_score"]

        return df

    def assign_segments(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Assign player segments based on rule-based logic combining
        RFM scores with behavioral signals.
        """
        df = df.copy()

        # Compute spending percentile for whale/dolphin classification
        payer_mask = df["monetary"] > 0
        if payer_mask.sum() > 0:
            df.loc[payer_mask, "spend_percentile"] = df.loc[payer_mask, "monetary"].rank(pct=True)
        df["spend_percentile"] = df["spend_percentile"].fillna(0)

        conditions = [
            # Whale: top 2% spenders AND high frequency
            (df["spend_percentile"] >= 0.98) & (df["f_score"] >= 3),

            # Dolphin: regular spender, moderate+ amounts
            (df["monetary"] > 0) & (df["m_score"] >= 3) & (df["spend_percentile"] < 0.98),

            # Minnow: occasional small purchases
            (df["monetary"] > 0) & (df["m_score"] < 3),

            # Churned: no activity for 7+ days
            (df["recency"] > 7),

            # At-Risk: declining activity, was active before
            (df["recency"] > 3) & (df["recency"] <= 7) & (df["r_score"] <= 2)
            & (df["frequency"] >= 3),

            # Engaged Free: active non-payer
            (df["monetary"] == 0) & (df["f_score"] >= 3) & (df["recency"] <= 3),

            # Casual: everyone else
        ]

        segments = [
            "whale", "dolphin", "minnow", "churned",
            "at_risk", "engaged_free",
        ]

        df["player_segment"] = np.select(conditions, segments, default="casual")

        return df

    def compute_segment_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute summary statistics per segment."""
        stats = df.groupby("player_segment").agg(
            user_count=("user_pseudo_id", "count"),
            pct_of_total=("user_pseudo_id", lambda x: len(x) / len(df) * 100),
            avg_revenue=("monetary", "mean"),
            total_revenue=("monetary", "sum"),
            revenue_share=("monetary", lambda x: x.sum() / max(df["monetary"].sum(), 1) * 100),
            avg_active_days=("frequency", "mean"),
            avg_recency=("recency", "mean"),
            avg_session_duration=("avg_session_duration", "mean"),
            avg_level=("max_level", "mean"),
            avg_purchases=("purchase_count", "mean"),
        ).round(2)

        stats = stats.sort_values("total_revenue", ascending=False)
        return stats

    def run_segmentation(
        self,
        df: Optional[pd.DataFrame] = None,
        write_to_bq: bool = True,
    ) -> Dict:
        """
        Run the full segmentation pipeline.

        Args:
            df: User metrics DataFrame (loads from BQ if None)
            write_to_bq: Write results back to BigQuery

        Returns:
            Dictionary with segment stats and user assignments
        """
        if df is None:
            df = self.load_user_metrics()

        logger.info("Computing RFM scores...")
        df = self.compute_rfm_scores(df)

        logger.info("Assigning segments...")
        df = self.assign_segments(df)

        # Compute stats
        stats = self.compute_segment_stats(df)
        logger.info(f"\nSegment Distribution:\n{stats.to_string()}")

        if write_to_bq:
            self._write_segments_to_bq(df)

        return {
            "segment_stats": stats,
            "user_segments": df[["user_pseudo_id", "player_segment", "r_score", "f_score", "m_score", "rfm_total"]],
            "total_users": len(df),
            "segment_counts": df["player_segment"].value_counts().to_dict(),
        }

    def _write_segments_to_bq(self, df: pd.DataFrame):
        """Write segmentation results to BigQuery."""
        from google.cloud import bigquery

        client = bigquery.Client(project=self.project_id)

        # Write detailed segment data
        segment_data = df[[
            "user_pseudo_id", "player_segment", "r_score", "f_score", "m_score",
            "rfm_total", "monetary", "frequency", "recency",
        ]].copy()
        segment_data["segmented_at"] = datetime.utcnow()

        table_ref = f"{self.project_id}.game_ml.player_segments"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        job = client.load_table_from_dataframe(
            segment_data, table_ref, job_config=job_config
        )
        job.result()
        logger.info(f"Wrote {len(segment_data)} segment assignments to {table_ref}")

        # Update dim_users
        update_query = f"""
        UPDATE `{self.project_id}.game_warehouse.dim_users` u
        SET u.player_segment = s.player_segment
        FROM `{self.project_id}.game_ml.player_segments` s
        WHERE u.user_pseudo_id = s.user_pseudo_id
        """
        client.query(update_query).result()
        logger.info("Updated dim_users with segment assignments")

        # Update mart_player_segments
        from src.transformation.sql_transforms import mart_player_segments
        client.query(mart_player_segments(self.project_id)).result()
        logger.info("Refreshed mart_player_segments")
