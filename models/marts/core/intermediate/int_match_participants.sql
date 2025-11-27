{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}

WITH matches AS (
    SELECT *
    FROM {{ ref("_stg_match_info__matches") }}
    {% if is_incremental() %}
        WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
),

participants AS (
    SELECT *
    FROM {{ ref("_stg_match_info__participants") }}
    {% if is_incremental() %}
        WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
),

-- 1. Obtenemos las métricas del mazo (Nivel y Elixir)
deck_metrics AS (
    SELECT
        bd.battle_id,
        bd.player_tag,
        AVG(bd.level) AS avg_card_level,
        AVG(c.cost) AS avg_elixir_cost
    FROM {{ ref("_stg_match_info__battle_decks") }} bd
    LEFT JOIN {{ ref("dim_card") }} c 
        ON bd.card_id = c.card_id
    {% if is_incremental() %}
        WHERE bd.load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
    GROUP BY 1, 2
)

SELECT
    p.*,
    m.arena_id,
    m.battle_time,
    dm.avg_card_level,
    dm.avg_elixir_cost
FROM participants p
LEFT JOIN matches m
    ON p.battle_id = m.id
LEFT JOIN deck_metrics dm
    ON p.battle_id = dm.battle_id
    AND p.player_tag = dm.player_tag 