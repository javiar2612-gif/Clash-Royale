WITH base AS (
    SELECT
        battle_id,
        arena_id,
        outcome,
        avg_card_level,
        avg_elixir_cost
    FROM {{ ref("fct_participation") }}
),

dim_arena AS (
    SELECT
        arena_id,
        arena_name
    FROM {{ ref("dim_arena") }}
),

battle_deltas AS (
    SELECT
        w.arena_id,
        w.battle_id,
        (w.avg_elixir_cost - l.avg_elixir_cost) AS delta_elixir,
        (w.avg_card_level - l.avg_card_level) AS delta_level
    FROM base w
    INNER JOIN base l
        ON w.battle_id = l.battle_id
    WHERE 
        w.outcome = 'winner' 
        AND l.outcome = 'loser'
)

SELECT
    da.arena_name,
    bd.arena_id,
    ROUND(AVG(bd.delta_elixir), 3) AS avg_delta_elixir,
    ROUND(AVG(bd.delta_level), 3) AS avg_delta_level,
    COUNT(bd.battle_id) AS total_battles
FROM battle_deltas bd
INNER JOIN dim_arena da
    ON bd.arena_id = da.arena_id
GROUP BY 1, 2
ORDER BY bd.arena_id