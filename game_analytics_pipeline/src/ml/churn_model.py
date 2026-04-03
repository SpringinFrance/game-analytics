"""
Churn Prediction Model
======================
Binary classification model to predict player churn (no session in next 7 days).
Uses XGBoost with features from the BigQuery feature store.

Pipeline:
    1. Load features from BigQuery feature_store
    2. Train/evaluate XGBoost model
    3. Generate predictions for all active users
    4. Write predictions back to BigQuery
    5. Log model metadata and metrics

Usage:
    # Training
    model = ChurnPredictor(project_id="your-project")
    model.train()
    model.evaluate()
    model.save_model("models/churn_v1.joblib")

    # Prediction
    model.predict_and_write()
"""

import logging
import json
import os
from datetime import datetime
from typing import Optional, Tuple, Dict

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    precision_recall_curve,
    f1_score,
    confusion_matrix,
)
from sklearn.preprocessing import LabelEncoder
import joblib

logger = logging.getLogger(__name__)


class ChurnPredictor:
    """XGBoost-based churn prediction model."""

    def __init__(
        self,
        project_id: str,
        model_path: str = "models/",
        test_size: float = 0.2,
        random_state: int = 42,
    ):
        self.project_id = project_id
        self.model_path = model_path
        self.test_size = test_size
        self.random_state = random_state

        self.model = None
        self.feature_columns = None
        self.label_encoders = {}
        self.metrics = {}

        os.makedirs(model_path, exist_ok=True)

    def load_features(self) -> pd.DataFrame:
        """Load feature store data from BigQuery."""
        from google.cloud import bigquery

        client = bigquery.Client(project=self.project_id)

        query = f"""
        SELECT *
        FROM `{self.project_id}.game_ml.feature_store`
        WHERE feature_computed_at = (
            SELECT MAX(feature_computed_at)
            FROM `{self.project_id}.game_ml.feature_store`
        )
        """

        logger.info("Loading features from BigQuery...")
        df = client.query(query).to_dataframe()
        logger.info(f"Loaded {len(df)} user feature records")
        return df

    def prepare_data(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.Series, list]:
        """
        Prepare features and labels for training.

        Returns:
            X: Feature matrix
            y: Target labels (1=churned, 0=active)
            feature_columns: List of feature names used
        """
        from .feature_engineering import get_feature_columns

        feature_columns = get_feature_columns()

        # Encode categorical features
        categorical_cols = ["platform", "geo_country"]
        for col in categorical_cols:
            if col in df.columns:
                le = LabelEncoder()
                df[f"{col}_encoded"] = le.fit_transform(df[col].fillna("unknown"))
                self.label_encoders[col] = le
                feature_columns.append(f"{col}_encoded")

        # Add boolean features
        if "is_payer" in df.columns:
            df["is_payer_int"] = df["is_payer"].astype(int)
            feature_columns.append("is_payer_int")

        # Filter to available columns
        available = [c for c in feature_columns if c in df.columns]
        missing = [c for c in feature_columns if c not in df.columns]
        if missing:
            logger.warning(f"Missing features: {missing}")

        X = df[available].fillna(0)
        y = df["is_churned"]

        self.feature_columns = available

        logger.info(f"Features: {len(available)} columns, {len(X)} samples")
        logger.info(f"Class distribution: {y.value_counts().to_dict()}")

        return X, y, available

    def train(
        self,
        df: Optional[pd.DataFrame] = None,
        xgb_params: Optional[dict] = None,
    ) -> Dict:
        """
        Train the churn prediction model.

        Args:
            df: Feature DataFrame (loads from BigQuery if None)
            xgb_params: XGBoost hyperparameters (uses defaults if None)

        Returns:
            Dictionary of evaluation metrics
        """
        try:
            from xgboost import XGBClassifier
        except ImportError:
            raise ImportError("xgboost is required. Install with: pip install xgboost")

        if df is None:
            df = self.load_features()

        X, y, feature_cols = self.prepare_data(df)

        # Train/test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.test_size,
            random_state=self.random_state, stratify=y
        )

        # Default XGBoost parameters optimized for churn prediction
        if xgb_params is None:
            # Calculate scale_pos_weight for class imbalance
            n_positive = y_train.sum()
            n_negative = len(y_train) - n_positive
            scale_pos_weight = n_negative / max(n_positive, 1)

            xgb_params = {
                "n_estimators": 200,
                "max_depth": 6,
                "learning_rate": 0.1,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "min_child_weight": 5,
                "scale_pos_weight": min(scale_pos_weight, 5),
                "eval_metric": "logloss",
                "random_state": self.random_state,
                "n_jobs": -1,
            }

        logger.info(f"Training XGBoost with params: {xgb_params}")

        self.model = XGBClassifier(**xgb_params)
        self.model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        # Evaluate
        self.metrics = self._evaluate(X_test, y_test)

        # Cross-validation
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=self.random_state)
        cv_scores = cross_val_score(self.model, X, y, cv=cv, scoring="roc_auc")
        self.metrics["cv_auc_mean"] = float(np.mean(cv_scores))
        self.metrics["cv_auc_std"] = float(np.std(cv_scores))

        logger.info(f"Training complete. Metrics: {self.metrics}")

        return self.metrics

    def _evaluate(self, X_test: pd.DataFrame, y_test: pd.Series) -> Dict:
        """Compute evaluation metrics on test set."""
        y_pred = self.model.predict(X_test)
        y_prob = self.model.predict_proba(X_test)[:, 1]

        auc = roc_auc_score(y_test, y_prob)
        f1 = f1_score(y_test, y_pred)
        cm = confusion_matrix(y_test, y_pred)
        report = classification_report(y_test, y_pred, output_dict=True)

        # Find the churned class key (can be 1, "1", or np.int64(1))
        churn_key = None
        for k in report.keys():
            if str(k) == "1":
                churn_key = k
                break
        if churn_key is None:
            churn_key = list(report.keys())[1]  # fallback: second class

        metrics = {
            "auc_roc": float(auc),
            "f1_score": float(f1),
            "precision": float(report[churn_key]["precision"]),
            "recall": float(report[churn_key]["recall"]),
            "accuracy": float(report["accuracy"]),
            "confusion_matrix": cm.tolist(),
            "test_size": len(y_test),
            "churn_rate": float(y_test.mean()),
        }

        logger.info(f"AUC-ROC: {auc:.4f}")
        logger.info(f"Precision: {metrics['precision']:.4f}")
        logger.info(f"Recall: {metrics['recall']:.4f}")
        logger.info(f"F1 Score: {f1:.4f}")

        return metrics

    def get_feature_importance(self, top_n: int = 20) -> pd.DataFrame:
        """Get top N most important features."""
        if self.model is None:
            raise ValueError("Model not trained yet")

        importance = pd.DataFrame({
            "feature": self.feature_columns,
            "importance": self.model.feature_importances_,
        }).sort_values("importance", ascending=False)

        return importance.head(top_n)

    def predict(self, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Generate churn predictions for all users.

        Returns:
            DataFrame with user_id, churn_probability, predicted_churn, risk_tier
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        if df is None:
            df = self.load_features()

        # Prepare features (same transformation as training)
        for col, le in self.label_encoders.items():
            if col in df.columns:
                df[f"{col}_encoded"] = df[col].map(
                    lambda x: le.transform([x])[0] if x in le.classes_ else -1
                )

        if "is_payer" in df.columns:
            df["is_payer_int"] = df["is_payer"].astype(int)

        X = df[self.feature_columns].fillna(0)

        # Predict
        churn_prob = self.model.predict_proba(X)[:, 1]

        # Support both user_id and user_pseudo_id (Firebase schema)
        user_id_col = "user_pseudo_id" if "user_pseudo_id" in df.columns else "user_id"

        predictions = pd.DataFrame({
            "user_pseudo_id": df[user_id_col],
            "churn_probability": churn_prob,
            "predicted_churn": (churn_prob >= 0.5).astype(int),
            "risk_tier": pd.cut(
                churn_prob,
                bins=[0, 0.2, 0.4, 0.6, 0.8, 1.0],
                labels=["very_low", "low", "medium", "high", "very_high"],
            ),
            "prediction_date": datetime.utcnow().date(),
            "model_version": self._get_model_version(),
        })

        logger.info(
            f"Generated predictions for {len(predictions)} users. "
            f"Churn rate: {predictions['predicted_churn'].mean():.2%}"
        )

        return predictions

    def predict_and_write(self, df: Optional[pd.DataFrame] = None):
        """Generate predictions and write to BigQuery."""
        from google.cloud import bigquery

        predictions = self.predict(df)

        client = bigquery.Client(project=self.project_id)

        # Write predictions
        table_ref = f"{self.project_id}.game_ml.predictions"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Replace daily
        )

        job = client.load_table_from_dataframe(
            predictions, table_ref, job_config=job_config
        )
        job.result()
        logger.info(f"Wrote {len(predictions)} predictions to {table_ref}")

        # Update dim_users with latest predictions
        # NOTE: Using CREATE OR REPLACE instead of UPDATE (DML blocked on BQ sandbox/free tier)
        update_query = f"""
        CREATE OR REPLACE TABLE `{self.project_id}.game_warehouse.dim_users` AS
        SELECT
            u.* EXCEPT(churn_probability),
            COALESCE(p.churn_probability, u.churn_probability) AS churn_probability,
        FROM `{self.project_id}.game_warehouse.dim_users` u
        LEFT JOIN `{self.project_id}.game_ml.predictions` p
            ON u.user_pseudo_id = p.user_pseudo_id
        """
        client.query(update_query).result()
        logger.info("Updated dim_users with churn predictions (CREATE OR REPLACE)")

        # Log model metadata
        self._log_model_metadata(client)

    def save_model(self, path: Optional[str] = None):
        """Save trained model to disk."""
        if self.model is None:
            raise ValueError("No model to save")

        if path is None:
            path = os.path.join(self.model_path, f"churn_model_{self._get_model_version()}.joblib")

        model_data = {
            "model": self.model,
            "feature_columns": self.feature_columns,
            "label_encoders": self.label_encoders,
            "metrics": self.metrics,
            "version": self._get_model_version(),
            "trained_at": datetime.utcnow().isoformat(),
        }

        joblib.dump(model_data, path)
        logger.info(f"Model saved to {path}")

    def load_model(self, path: str):
        """Load a previously trained model."""
        model_data = joblib.load(path)

        self.model = model_data["model"]
        self.feature_columns = model_data["feature_columns"]
        self.label_encoders = model_data["label_encoders"]
        self.metrics = model_data["metrics"]

        logger.info(f"Model loaded from {path} (version: {model_data['version']})")

    def _get_model_version(self) -> str:
        """Generate model version string."""
        return datetime.utcnow().strftime("v%Y%m%d")

    def _log_model_metadata(self, client):
        """Log model training metadata to BigQuery."""
        from google.cloud import bigquery

        metadata = pd.DataFrame([{
            "model_version": self._get_model_version(),
            "model_type": "xgboost",
            "trained_at": datetime.utcnow(),
            "metrics_json": json.dumps(self.metrics),
            "feature_count": len(self.feature_columns),
            "auc_roc": self.metrics.get("auc_roc", 0),
            "f1_score": self.metrics.get("f1_score", 0),
            "precision": self.metrics.get("precision", 0),
            "recall": self.metrics.get("recall", 0),
        }])

        table_ref = f"{self.project_id}.game_ml.model_metadata"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

        job = client.load_table_from_dataframe(
            metadata, table_ref, job_config=job_config
        )
        job.result()
        logger.info(f"Model metadata logged to {table_ref}")
