{{ config(materialized='table') }}

SELECT
    id AS arena_id,
    arena_name
FROM {{ ref("_stg_match_info__arenas") }}
