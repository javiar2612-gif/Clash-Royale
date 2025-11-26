{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}

WITH participants AS (
    SELECT *
    FROM {{ ref("int_match_participants") }}
    {% if is_incremental() %}
        WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
),

battle_decks AS (
    SELECT *
    FROM {{ ref("_stg_match_info__battle_decks") }}
    {% if is_incremental() %}
        WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
)

SELECT
    d.id,
    d.battle_id,
    d.player_tag,
    d.card_id,
    d.level,
    p.outcome,
    p.starting_trophies,
    p.crowns,
    p.arena_id,
    p.battle_time,
    d.load_date
FROM battle_decks d
LEFT JOIN  participants p
    ON p.battle_id = d.battle_id



