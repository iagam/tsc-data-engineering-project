USERS_USER_UPSERT = """
MERGE `sample_dataset_5.users_user` T
USING
(

  SELECT
    JSON_VALUE(item, '$.login.uuid') as user_id,
    JSON_VALUE(item, '$.gender') as gender,
    JSON_VALUE(item, '$.name.title') as title,
    JSON_VALUE(item, '$.name.first') as first_name,
    JSON_VALUE(item, '$.name.last') as last_name,
    CAST(JSON_VALUE(item, '$.dob.date') AS TIMESTAMP) as dob,
    CAST(JSON_VALUE(item, '$.dob.age') AS INT64) as age,
    JSON_VALUE(item, '$.nat') as nationality,
    ingested_at

  FROM `sample_dataset_5.raw_api_data`,
  UNNEST(JSON_QUERY_ARRAY(raw_json, '$.results')) AS item
  WHERE ingested_at > (select coalesce(max(ingested_at),'1900-01-01') from `sample_dataset_5.users_user`)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(item, '$.login.uuid')
    ORDER BY ingested_at DESC
) = 1

) S

ON T.user_id = S.user_id

WHEN MATCHED THEN

UPDATE SET
gender = S.gender,
title = S.title,
first_name = S.first_name,
last_name = S.last_name,
dob = S.dob,
age = S.age,
nationality = S.nationality,
ingested_at = S.ingested_at

WHEN NOT MATCHED THEN
  INSERT (user_id, gender, title, first_name, last_name, dob, age, nationality, ingested_at)
  VALUES (user_id, gender, title, first_name, last_name, dob, age, nationality, ingested_at);
"""

USERS_LOGIN_UPSERT = """
MERGE `sample_dataset_5.users_login` T
USING (
  SELECT
    JSON_VALUE(item, '$.login.uuid') as user_id,
    JSON_VALUE(item, '$.login.username') as username,
    JSON_VALUE(item, '$.login.md5') as password_hash,
    CAST(JSON_VALUE(item, '$.registered.date') AS TIMESTAMP) as registered_date,
    ingested_at
  FROM `sample_dataset_5.raw_api_data`,
  UNNEST(JSON_QUERY_ARRAY(raw_json, '$.results')) AS item
  WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM `sample_dataset_5.users_login`)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(item, '$.login.uuid')
    ORDER BY ingested_at DESC
  ) = 1
) S
ON T.user_id = S.user_id
WHEN MATCHED THEN
  UPDATE SET
    username = S.username,
    password_hash = S.password_hash,
    registered_date = S.registered_date,
    ingested_at = S.ingested_at
WHEN NOT MATCHED THEN
  INSERT (user_id, username, password_hash, registered_date, ingested_at)
  VALUES (user_id, username, password_hash, registered_date, ingested_at);
"""

USERS_LOCATION_UPSERT = """
MERGE `sample_dataset_5.users_location` T
USING (
  SELECT
    JSON_VALUE(item, '$.login.uuid') as user_id,
    CAST(JSON_VALUE(item, '$.location.street.number') AS INT64) as street_number,
    JSON_VALUE(item, '$.location.street.name') as street_name,
    JSON_VALUE(item, '$.location.city') as city,
    JSON_VALUE(item, '$.location.state') as state,
    JSON_VALUE(item, '$.location.country') as country,
    JSON_VALUE(item, '$.location.postcode') as postcode,
    CAST(JSON_VALUE(item, '$.location.coordinates.latitude') AS FLOAT64) as latitude,
    CAST(JSON_VALUE(item, '$.location.coordinates.longitude') AS FLOAT64) as longitude,
    ingested_at
  FROM `sample_dataset_5.raw_api_data`,
  UNNEST(JSON_QUERY_ARRAY(raw_json.results)) AS item
  WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM `sample_dataset_5.users_location`)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(item, '$.login.uuid')
    ORDER BY ingested_at DESC
  ) = 1
) S
ON T.user_id = S.user_id
WHEN MATCHED THEN
  UPDATE SET
    street_number = S.street_number, street_name = S.street_name, city = S.city,
    state = S.state, country = S.country, postcode = S.postcode,
    latitude = S.latitude, longitude = S.longitude,
    ingested_at = S.ingested_at
WHEN NOT MATCHED THEN
  INSERT (user_id, street_number, street_name, city, state, country, postcode, latitude, longitude, ingested_at)
  VALUES (user_id, street_number, street_name, city, state, country, postcode, latitude, longitude, ingested_at);
"""

USERS_CONTACT_UPSERT = """
MERGE `sample_dataset_5.users_contact` T
USING (
  SELECT
    JSON_VALUE(item, '$.login.uuid') as user_id,
    JSON_VALUE(item, '$.email') as email,
    JSON_VALUE(item, '$.phone') as phone,
    JSON_VALUE(item, '$.cell') as cell,
    ingested_at
  FROM `sample_dataset_5.raw_api_data`,
  UNNEST(JSON_QUERY_ARRAY(raw_json, '$.results')) AS item
  WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM `sample_dataset_5.users_contact`)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(item, '$.login.uuid')
    ORDER BY ingested_at DESC
  ) = 1
) S
ON T.user_id = S.user_id
WHEN MATCHED THEN
  UPDATE SET
    email = S.email, phone = S.phone, cell = S.cell,
    ingested_at = S.ingested_at
WHEN NOT MATCHED THEN
  INSERT (user_id, email, phone, cell, ingested_at)
  VALUES (user_id, email, phone, cell, ingested_at);
"""

USERS_ASSETS_UPSERT = """
MERGE `sample_dataset_5.users_assets` T
USING (
  SELECT
    JSON_VALUE(item, '$.login.uuid') as user_id,
    JSON_VALUE(item, '$.picture.large') as picture_large,
    JSON_VALUE(item, '$.picture.medium') as picture_medium,
    JSON_VALUE(item, '$.picture.thumbnail') as picture_thumbnail,
    ingested_at
  FROM `sample_dataset_5.raw_api_data`,
  UNNEST(JSON_QUERY_ARRAY(raw_json, '$.results')) AS item
  WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM `sample_dataset_5.users_assets`)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(item, '$.login.uuid')
    ORDER BY ingested_at DESC
  ) = 1
) S
ON T.user_id = S.user_id
WHEN MATCHED THEN
  UPDATE SET
    picture_large = S.picture_large,
    picture_medium = S.picture_medium,
    picture_thumbnail = S.picture_thumbnail,
    ingested_at = S.ingested_at
WHEN NOT MATCHED THEN
  INSERT (user_id, picture_large, picture_medium, picture_thumbnail, ingested_at)
  VALUES (user_id, picture_large, picture_medium, picture_thumbnail, ingested_at);
"""
