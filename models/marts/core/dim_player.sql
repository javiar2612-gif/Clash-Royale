{{ config(materialized='table') }}

SELECT
    tag AS player_id,
    current_clan_tag AS clan_id
FROM {{ ref("_stg_match_info__players") }}
