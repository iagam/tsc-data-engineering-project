from google.cloud import bigquery

RAW_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("raw_json", "JSON"),
]

AUDIT_LOG_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("step_name", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("start_time", "TIMESTAMP"),
    bigquery.SchemaField("end_time", "TIMESTAMP"),
    bigquery.SchemaField("error_message", "STRING"),
]

METADATA_INFO_SCHEMA = [
    bigquery.SchemaField("table_name", "STRING"),
    bigquery.SchemaField("column_name", "STRING"),
    bigquery.SchemaField("data_type", "STRING"),
    bigquery.SchemaField("is_nullable", "BOOLEAN"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

METRICS_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("table_name", "STRING"),
    bigquery.SchemaField("records_processed", "INT64"),
    bigquery.SchemaField("processing_time_seconds", "FLOAT64"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

USERS_USER_SCHEMA = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("dob", "TIMESTAMP"),
    bigquery.SchemaField("age", "INT64"),
    bigquery.SchemaField("nationality", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

USERS_LOGIN_SCHEMA = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("username", "STRING"),
    bigquery.SchemaField("password_hash", "STRING"),
    bigquery.SchemaField("registered_date", "TIMESTAMP"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

USERS_LOCATION_SCHEMA = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED", ),
    bigquery.SchemaField("street_number", "INTEGER"),
    bigquery.SchemaField("street_name", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("postcode", "STRING"),
    bigquery.SchemaField("latitude", "FLOAT"),
    bigquery.SchemaField("longitude", "FLOAT"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

USERS_CONTACT_SCHEMA = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("phone", "STRING"),
    bigquery.SchemaField("cell", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

USERS_ASSETS_SCHEMA = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("picture_large", "STRING"),
    bigquery.SchemaField("picture_medium", "STRING"),
    bigquery.SchemaField("picture_thumbnail", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]

TABLE_SCHEMAS = {
    "raw_api_data": RAW_SCHEMA,
    "pipeline_audit_logs": AUDIT_LOG_SCHEMA,
    "pipeline_metrics": METRICS_SCHEMA,
    "metadata_info": METADATA_INFO_SCHEMA,
    "users_user": USERS_USER_SCHEMA,
    "users_location": USERS_LOCATION_SCHEMA,
    "users_contact": USERS_CONTACT_SCHEMA,
    "users_login": USERS_LOGIN_SCHEMA,
    "users_assets": USERS_ASSETS_SCHEMA,
}
