{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT
    *
    FROM 
        {{ source('match_info','arenas') }}
),

transformed AS (
    SELECT
        arena_id::INTEGER AS id,
        arena_name ::VARCHAR AS arena_name
    FROM source
)

SELECT * FROM transformed