from datetime import datetime, timezone
import json


def load_raw_data(client, dataset: str, run_id: str, data: dict, raw_table: str):
    """
    Loads raw JSON data into BigQuery using batch load.

    Args:
        client: BigQuery client
        dataset (str): Dataset name
        run_id (str): Unique pipeline run ID
        data (dict): API response
        raw_table (str): Target table
    """
    table_id = f"{client.project}.{dataset}.{raw_table}"

    rows = [
        {
            "run_id": 2,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "raw_json": data,
        }
    ]

    try:
        job = client.load_table_from_json(rows, table_id)
        job.result()
    except Exception as e:
        raise RuntimeError(f"BigQuery load failed: {str(e)}")
