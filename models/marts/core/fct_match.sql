{{ config(materialized='table') }}

WITH matches AS (
    SELECT *
    FROM {{ ref("_stg_match_info__matches") }}
),

participants AS (
    SELECT *
    FROM {{ ref('_stg_match_info__participants') }}
),

players AS (
    SELECT *
    FROM {{ ref("dim_player") }}
),

final AS (
    SELECT
        p.battle_id,
        pl.player_id,
        pl.clan_id,
        m.arena_id AS arena_id,
        m.battle_time,
        DATE_TRUNC('hour', m.battle_time)::TIMESTAMP_NTZ AS date_hour_id,

        p.outcome,
        p.starting_trophies,
        p.trophy_change,
        p.crowns,


    FROM participants p
    INNER JOIN matches m
        ON p.id = m.id
    INNER JOIN players pl
        ON p.player_tag = pl.player_id
)

SELECT *
FROM final
