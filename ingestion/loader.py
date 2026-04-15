from datetime import datetime, timezone
import json


def insert_raw_data(client, dataset: str, run_id: str, data: dict, raw_table: str):
    """
    Inserts raw JSON data into a BigQuery table with run_id and ingested_at timestamp.

    Args:
        client (google.cloud.bigquery.Client): BigQuery client instance
        dataset (str): Dataset containing the target table
        run_id (str/int): Unique identifier for the current pipeline run
        data (dict): Raw data/JSON payload to be stored
        raw_table (str): Destination table

    Returns:
        None

    Raises:
        Exception: If the BigQuery insert operation encounters errors, detailing the specific failure messages.
    """
    table_id = f"{client.project}.{dataset}.{raw_table}"

    rows = [
        {
            "run_id": run_id,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "raw_json": json.dumps(data),
        }
    ]

    errors = client.insert_rows_json(table_id, rows)

    if errors:
        error_messages = []

        for err in errors:
            for e in err.get("errors", []):
                error_messages.append(e.get("message", "Error Unknown"))

        raise Exception(f"BigQuery insert failed: {error_messages}")
