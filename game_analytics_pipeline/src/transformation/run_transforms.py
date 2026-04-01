"""
Transformation Runner
=====================
Executes SQL transformations in the correct order.
Can be triggered by Cloud Scheduler or run manually.

Usage:
    python -m src.transformation.run_transforms --project your-project --layer all
    python -m src.transformation.run_transforms --project your-project --layer staging
    python -m src.transformation.run_transforms --project your-project --layer marts
"""

import argparse
import logging
import time
from typing import Optional

from google.cloud import bigquery

from .sql_transforms import get_all_transforms

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def run_transforms(
    project_id: str,
    layer: str = "all",
    dry_run: bool = False,
):
    """
    Execute SQL transformations for specified layer(s).

    Args:
        project_id: GCP project ID
        layer: 'staging', 'warehouse', 'marts', or 'all'
        dry_run: If True, print SQL without executing
    """
    client = bigquery.Client(project=project_id)
    all_transforms = get_all_transforms(project_id)

    # Define execution order
    if layer == "all":
        layers_to_run = ["staging", "warehouse", "marts"]
    elif layer in all_transforms:
        layers_to_run = [layer]
    else:
        raise ValueError(f"Unknown layer: {layer}. Choose from: staging, warehouse, marts, all")

    # Ensure datasets exist
    for dataset_name in ["game_staging", "game_warehouse", "game_marts"]:
        dataset_ref = f"{project_id}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)

    total_start = time.time()
    results = {}

    for current_layer in layers_to_run:
        logger.info(f"{'=' * 60}")
        logger.info(f"Running {current_layer.upper()} transformations")
        logger.info(f"{'=' * 60}")

        transforms = all_transforms[current_layer]

        for table_name, sql in transforms.items():
            logger.info(f"  Building {table_name}...")

            if dry_run:
                logger.info(f"  [DRY RUN] SQL:\n{sql[:200]}...")
                results[table_name] = {"status": "dry_run"}
                continue

            try:
                start = time.time()
                job = client.query(sql)
                job.result()  # Wait for completion
                duration = time.time() - start

                # Get row count
                dest_table = f"{project_id}.game_{current_layer}.{table_name}"
                try:
                    table = client.get_table(dest_table)
                    row_count = table.num_rows
                except Exception:
                    row_count = "unknown"

                logger.info(
                    f"  {table_name}: {row_count} rows ({duration:.1f}s)"
                )
                results[table_name] = {
                    "status": "success",
                    "rows": row_count,
                    "duration_sec": round(duration, 1),
                }

            except Exception as e:
                logger.error(f"  FAILED {table_name}: {e}")
                results[table_name] = {"status": "error", "error": str(e)}

    total_duration = time.time() - total_start
    logger.info(f"\nTotal transformation time: {total_duration:.1f}s")
    logger.info(f"Results: {results}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data transformations")
    parser.add_argument("--project", required=True, help="GCP Project ID")
    parser.add_argument("--layer", default="all", help="Layer to run: staging, warehouse, marts, all")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")

    args = parser.parse_args()
    run_transforms(args.project, args.layer, args.dry_run)
