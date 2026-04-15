import uuid
from utils.config_loader import load_config
from utils.bigquery_client import get_bigquery_client
from utils.logger import log_step_start, log_step_end
from ingestion.api_client import fetch_api_data
from ingestion.loader import insert_raw_data
from transformations.transform import execute_transformations
from transformations.upserts import USERS_ASSETS_UPSERT, USERS_USER_UPSERT, USERS_LOGIN_UPSERT, USERS_LOCATION_UPSERT, USERS_CONTACT_UPSERT

def main():

    config = load_config()
    run_id = str(uuid.uuid4())

    client = get_bigquery_client(
        service_account_path=config["service_account_path"],
        project_id=config["project_id"]
    )
    project = config["project_id"]
    dataset = config["dataset"]

    try:

        # API FETCH
        step = "API_FETCH"
        print(step)
        log_step_start(client, project, dataset, run_id, step)

        data = fetch_api_data(
            url=config["api"]["url"],
            params=config["api"]["params"],
            retries=config["pipeline"]["max_retries"],
            delay=config["pipeline"]["retry_delay"]
        )
        print('Done')
        print()
        log_step_end(client, project, dataset, run_id, step, "SUCCESS")

        # INGESTION
        step = "INGESTION"
        print(step)
        log_step_start(client, project, dataset, run_id, step)

        insert_raw_data(
            client=client,
            project_id=project,
            dataset=dataset,
            run_id=run_id,
            data=data,
            raw_table=config["tables"]["raw_table"]
        )
        print("Done")
        print()
        log_step_end(client, project, dataset, run_id, step, "SUCCESS")

        # TRANSFORMATION
        print("TRANSFORMATION")
        steps = [
            ("users_user", USERS_USER_UPSERT),
            ("users_login", USERS_LOGIN_UPSERT),
            ("users_location", USERS_LOCATION_UPSERT),
            ("users_contact", USERS_CONTACT_UPSERT),
            ("users_assets", USERS_ASSETS_UPSERT),
        ]

        execute_transformations(client, project, dataset, run_id, steps)

        print("Done")

    except Exception as e:
        print("Pipeline failed:", e)
        log_step_end(client, project, dataset, run_id, step, "FAILED")
        raise

    return data


if __name__ == "__main__":
    main()
