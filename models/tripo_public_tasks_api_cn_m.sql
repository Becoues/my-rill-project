WITH base AS (
  SELECT
    id,
    task_id,
    client_id,
    type,
    status,
    input,
    storage,
    output,
    CASE
      WHEN LOWER(TRIM(CAST(is_delete AS VARCHAR))) IN ('1', 'true', 't', 'yes', 'y') THEN TRUE
      ELSE FALSE
    END AS is_delete,
    extra,
    CAST(TRY_STRPTIME(NULLIF(create_time, ''), '%Y-%m-%d %H:%M:%S.%f %z') AS TIMESTAMPTZ) AS create_time,
    CAST(TRY_STRPTIME(NULLIF(start_time, ''), '%Y-%m-%d %H:%M:%S.%f %z') AS TIMESTAMPTZ) AS start_time,
    CAST(TRY_STRPTIME(NULLIF(end_time, ''), '%Y-%m-%d %H:%M:%S.%f %z') AS TIMESTAMPTZ) AS end_time,
    CAST(TRY_STRPTIME(NULLIF(update_time, ''), '%Y-%m-%d %H:%M:%S.%f %z') AS TIMESTAMPTZ) AS update_time,
    txn_id
  FROM tripo_public_tasks_api_cn
)
SELECT
  base.*,
  CAST(json_extract(base.input, '$.pbr') AS BOOLEAN) AS input_pbr,
  json_extract_string(base.input, '$.file.type') AS input_file_type,
  json_extract_string(base.input, '$.file.object.key') AS input_file_key,
  json_extract_string(base.input, '$.file.object.bucket') AS input_file_bucket,
  CAST(json_extract(base.input, '$.quad') AS BOOLEAN) AS input_quad,
  CAST(json_extract(base.input, '$.texture') AS BOOLEAN) AS input_texture,
  json_extract_string(base.input, '$.model_version') AS input_model_version,
  CAST(json_extract(base.input, '$.smart_low_poly') AS BOOLEAN) AS input_smart_low_poly,
  json_extract_string(base.input, '$.texture_quality') AS input_texture_quality,
  json_extract_string(base.input, '$.geometry_quality') AS input_geometry_quality,
  json_extract_string(base.extra, '$.reason') AS reason,
  CASE
    WHEN base.start_time IS NOT NULL AND base.end_time IS NOT NULL
      THEN DATEDIFF('second', base.start_time, base.end_time)
    ELSE NULL
  END AS duration_seconds
FROM base