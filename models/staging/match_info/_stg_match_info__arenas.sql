{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT DISTINCT
        arena_id
    FROM 
        {{ source('match_info','match_data') }}
    WHERE arena_id IS NOT NULL
),

transformed AS (
    SELECT
        arena_id::INTEGER AS id,
        NULL::VARCHAR AS name
    FROM source
)

SELECT * FROM transformed