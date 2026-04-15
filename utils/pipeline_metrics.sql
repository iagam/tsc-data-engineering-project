INSERT INTO `devx-tsc.sample_dataset_5.pipeline_metrics`
(run_id, start_time, end_time, extraction_duration, load_duration, transformation_duration, status)

SELECT * FROM (

  WITH base AS (
    SELECT
      run_id,
      step_name,
      status,
      start_time,
      end_time,
      TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration
    FROM `devx-tsc.sample_dataset_5.pipeline_audit_logs`
    WHERE status IN ('SUCCESS', 'FAILED')
      AND run_id NOT IN (
        SELECT DISTINCT run_id
        FROM `devx-tsc.sample_dataset_5.pipeline_metrics`
      )
  ),

  aggregated AS (
    SELECT
      run_id,
      MIN(start_time) AS start_time,
      MAX(end_time) AS end_time,
      SUM(CASE WHEN step_name = 'EXTRACT' THEN duration ELSE 0 END) AS extraction_duration,
      SUM(CASE WHEN step_name = 'LOAD' THEN duration ELSE 0 END) AS load_duration,

      SUM(CASE
          WHEN step_name NOT IN ('EXTRACT', 'LOAD')
          THEN duration
          ELSE 0
      END) AS transformation_duration,

      CASE
        WHEN MAX(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) = 1 THEN 'FAILED'
        ELSE 'SUCCESS'
      END AS status

    FROM base
    GROUP BY run_id
  )

  SELECT
    run_id,
    start_time,
    end_time,
    extraction_duration,
    load_duration,
    transformation_duration,
    status
  FROM aggregated

) sub;