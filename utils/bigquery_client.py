from google.cloud import bigquery
from google.oauth2 import service_account


def get_bigquery_client(service_account_path: str, project_id: str):
    """
    Initializes a BigQuery client using a service account JSON file.

    Args:
        service_account_path (str): The file path to the service account key
        project_id (str): Google Cloud project ID

    Returns:
        google.cloud.bigquery.client.Client: An authenticated BigQuery client
            instance.
    """
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=project_id)
