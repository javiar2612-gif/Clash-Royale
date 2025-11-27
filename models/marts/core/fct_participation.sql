{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref("int_match_participants") }} AS b
    {% if is_incremental() %}
        WHERE b.load_date > (SELECT MAX(t.load_date) FROM {{ this }} AS t)
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
        clan_id,
        clan_badge_id,
        last_seen_battle_time
    FROM {{ ref("dim_clan") }}
)

SELECT
    b.id,
    b.battle_id,
    pl.player_id,
    pl.clan_id,
    b.arena_id,
    d.date_hour_id,
    b.outcome,
    b.starting_trophies,
    b.trophy_change,
    b.crowns,
    c.clan_badge_id,
    c.last_seen_battle_time,
    b.avg_card_level,
    b.avg_elixir_cost,
    b.load_date
FROM base b
LEFT JOIN players pl
    ON b.player_tag = pl.player_id
LEFT JOIN dates d
    ON DATE_TRUNC('hour', b.battle_time)::TIMESTAMP_NTZ = d.date_hour_id
LEFT JOIN clans c
    ON pl.clan_id = c.clan_id
LEFT JOIN arenas a
    ON b.arena_id = a.arena_id
