import time
from utils.logger import log_step_start, log_step_end


def run_query(client, query: str):
    """
    Run a sql query on BigQuery
    """
    job = client.query(query)
    job.result()
    return job


def execute_transformations(
    client, project: str, dataset: str, run_id: str, steps: list
):
    """
    Executes a sequence of transformation queries in BigQuery with step-level logging.

    This function iterates over a list of transformation steps, where each step
    consists of a step name and its corresponding SQL query. For each step, it:
    - Logs the start of execution
    - Executes the query in BigQuery
    - Logs success or failure along with execution status

    If any step fails, the error is logged and the exception is raised to stop
    further execution of the pipeline.

    Args:
        client (google.cloud.bigquery.Client): BigQuery client instance.
        project (str): GCP project ID.
        dataset (str): BigQuery dataset containing target tables.
        run_id (str): Unique identifier for the current pipeline run.
        steps (list[tuple[str, str]]): List of transformation steps, where each
            element is a tuple of (step_name, sql_query).
    """
    for step, query in steps:
        try:
            start = time.time()

            log_step_start(client, project, dataset, run_id, step)

            job = run_query(client, query)

            duration = time.time() - start

            log_step_end(client, project, dataset, run_id, step, "SUCCESS")

        except Exception as e:
            log_step_end(client, project, dataset, run_id, step, "FAILED", str(e))
            raise
