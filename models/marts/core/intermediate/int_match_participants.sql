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
)

SELECT
    p.*,
    m.arena_id,
    m.battle_time
FROM participants p
LEFT JOIN matches m
    ON p.battle_id = m.id
