{{ config(materialized='table') }}

SELECT
    clan_tag AS clan_id,
    clan_badge_id,
    last_seen_battle_time
FROM {{ ref('_stg_match_info__clans') }}
