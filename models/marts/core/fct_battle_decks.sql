{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='ignore'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref("int_match_participants_battle_decks") }}
    {% if is_incremental() %}
        WHERE battle_time > (SELECT MAX(battle_time) FROM {{ this }})
    {% endif %}
),

players AS (
    SELECT *
    FROM {{ ref("dim_player") }}
),

dates AS (
    SELECT *
    FROM {{ ref("dim_date") }}
),

arenas AS (
    SELECT *
    FROM {{ ref("dim_arena") }}
),

clans AS (
    SELECT
        *
    FROM {{ ref("dim_clan") }}
),

cards AS (
    SELECT
        *
    FROM {{ref("dim_card")}}
)

SELECT
    b.id,
    b.battle_id,
    b.player_tag AS player_id,
    b.card_id,
    b.level,
    b.outcome,
    b.starting_trophies,
    b.crowns,
    b.arena_id,
    b.battle_time,

    pl.clan_id,
    d.date_hour_id,
    b.load_date

FROM base b
LEFT JOIN players pl
    ON pl.player_id = b.player_tag
LEFT JOIN arenas a
    ON a.arena_id = b.arena_id
LEFT JOIN dates d
    ON DATE_TRUNC('hour', b.battle_time)::TIMESTAMP_NTZ = d.date_hour_id
LEFT JOIN clans c
    ON c.clan_id = pl.clan_id
LEFT JOIN cards ca
    ON ca.card_id = b.card_id

