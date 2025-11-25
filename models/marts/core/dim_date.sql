{{ config(materialized='table') }}

WITH truncated AS (
    SELECT DISTINCT
        DATE_TRUNC('hour', battle_time)::TIMESTAMP_NTZ AS date_hour
    FROM {{ ref("_stg_match_info__matches") }}
),

final AS (
    SELECT
        date_hour                             AS date_hour_id,
        EXTRACT(YEAR    FROM date_hour)       AS year,
        EXTRACT(QUARTER FROM date_hour)       AS quarter,
        EXTRACT(MONTH   FROM date_hour)       AS month,
        EXTRACT(WEEK    FROM date_hour)       AS week,
        TRIM(TO_CHAR(date_hour, 'Day'))       AS weekday,
        EXTRACT(DAY     FROM date_hour)       AS day,
        EXTRACT(HOUR    FROM date_hour)       AS hour
    FROM truncated
)

SELECT *
FROM final
ORDER BY date_hour_id
