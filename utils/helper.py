import os
from dotenv import load_dotenv
import yaml


def run_sql_file(client, file_path):
    with open(file_path, "r") as f:
        query = f.read()

    job = client.query(query)
    job.result()

    return job


def run_query(client, query: str):
    """
    Run a sql query on BigQuery
    """
    job = client.query(query)
    job.result()
    return job


def load_config():
    load_dotenv()

    with open("config/config.yml", "r") as f:
        config = yaml.safe_load(f)

    config["service_account_path"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    return config
