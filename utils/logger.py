from google.cloud import bigquery

def log_step_start(client, dataset: str, run_id: str, step: str):
    """
    Starts the logging proocess of a task and inserts into
    pipeline_audit_logs table.
    """

    query = f"""
    INSERT INTO `{client.project}.{dataset}.pipeline_audit_logs`
    (run_id, step_name, status, start_time, end_time, error_message)
    VALUES (@run_id, @step, 'STARTED', CURRENT_TIMESTAMP(), NULL, NULL)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("step", "STRING", step),
        ]
    )

    client.query(query, job_config=job_config).result()


def log_step_end(client, dataset: str, run_id: str, step: str, status: str, error: str=None):
    """
    Updates the logs of the started step in success or failure depending on
    the run and also logs error when there.
    """
    query = f"""
    UPDATE `{client.project}.{dataset}.pipeline_audit_logs`
    SET status = @status,
        end_time = CURRENT_TIMESTAMP(),
        error_message = @error
    WHERE run_id = @run_id
      AND step_name = @step
      AND status = 'STARTED'
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("error", "STRING", error),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("step", "STRING", step),
        ]
    )

    client.query(query, job_config=job_config).result()
