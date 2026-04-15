import time
from utils.logger import log_step_start, log_step_end
# from utils.metrics import log_metrics


def run_query(client, query):
    job = client.query(query)
    job.result()
    return job


def execute_transformations(client, project, dataset, run_id, steps):
    for step, query in steps:
        try:
            start = time.time()

            log_step_start(client, project, dataset, run_id, step)

            job = run_query(client, query)

            duration = time.time() - start

            log_step_end(client, project, dataset, run_id, step, "SUCCESS")

            # Get affected rows
            query_stats = job._properties.get("statistics", {}).get("query", {}).get("dmlStats", {})

            inserted = int(query_stats.get("insertedRowCount", 0))
            updated = int(query_stats.get("updatedRowCount", 0))
            deleted = int(query_stats.get("deletedRowCount", 0))
            affected_row_count = inserted + updated + deleted

            # log_metrics(client, dataset, run_id, step, count, duration)

        except Exception as e:
            log_step_end(client, project, dataset, run_id, step, "FAILED", str(e))
            raise
