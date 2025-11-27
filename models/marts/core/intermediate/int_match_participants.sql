{{
    config(
        materialized='view'
    )
}}

WITH matches AS (
    SELECT *
    FROM {{ ref("_stg_match_info__matches") }}
    -- Incremental WHERE clause removed
),

participants AS (
    SELECT *
    FROM {{ ref("_stg_match_info__participants") }}
    -- Incremental WHERE clause removed
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
    -- Incremental WHERE clause removed
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