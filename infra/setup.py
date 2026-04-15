from google.cloud import bigquery
from utils.config_loader import load_config
from utils.bigquery_client import get_bigquery_client
from infra.schemas import TABLE_SCHEMAS


def create_dataset(client, dataset, location):
    """
    Checks if a BigQuery dataset exists and creates it if it does not.

    Args:
        client (google.cloud.bigquery.Client): BigQuery client instance
        dataset (str): Name of the dataset to create
        location (str): The geographic location (e.g., 'US', 'EU') for the dataset
    """
    dataset_ref = bigquery.Dataset(f"{client.project}.{dataset}")
    dataset_ref.location = location

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset}' already exists, skipping creation")
    except:
        client.create_dataset(dataset_ref)
        print(f"Dataset '{dataset}' created successfully")


def create_table(client, dataset, table_name, schema):
    """
    Checks if a BigQuery table exists and creates it with the provided schema if missing.

    Args:
        client (google.cloud.bigquery.Client): BigQuery client instance
        dataset (str): Dataset where the table should reside
        table_name (str): Name of the table to create
        schema (list): List of bigquery.SchemaField objects defining the table structure
    """
    table_id = f"{client.project}.{dataset}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)

    try:
        client.get_table(table_id)
        print(f"Table '{table_name}' already exists, skipping creation")
    except Exception:
        client.create_table(table)
        print(f"Table '{table_name}' created successfully")


def run_sql_file(client, file_path):
    with open(file_path, "r") as f:
        query = f.read()

    job = client.query(query)
    job.result()

    return job


def main():
    """
    Orchestrates the setup of the BigQuery infra required for the project by loading
    configuration from the config.
    """
    config = load_config()
    client = get_bigquery_client(
        config["service_account_path"],
        config["project_id"]
        )
    dataset = config["dataset"]

    # create_dataset(client, dataset, config["location"])

    # SYSTEM TABLES
    system_tables = config["tables"]["system_tables"]
    for table in system_tables:
        create_table(client, dataset, table, TABLE_SCHEMAS[table])

    # RAW TABLE
    raw_table = config["tables"]["raw_table"]
    create_table(client, dataset, raw_table, TABLE_SCHEMAS[raw_table])

    # NORMALIZED TABLES
    for table in config["tables"]["normalized_tables"]:
        create_table(client, dataset, table, TABLE_SCHEMAS[table])

    run_sql_file(client, "infra/metadata_info.sql")
    print("Metadata created")


if __name__ == "__main__":
    main()
