{{ config(
    materialized='view'
) 
}}

WITH stg_battles_base AS (
    -- Selecciona las columnas exactas que se encuentran en el diagrama 'battles'
    -- Se utilizan alias si es necesario para simplificar los nombres de las columnas
    SELECT
        id,
        tournamentTag AS tournament_tag,
        battleTime AS battle_time,
        arena_id,
        gameMode_id AS game_mode_id,
        average_startingTrophies AS avg_starting_trophies
    FROM 
        {{ source('match_info','match_data') }}
),

stg_battles_typed AS (
    -- Casteo de tipos para coincidir con la tabla 'battles' (VARCHAR, DATETIME, INT)
    SELECT
        id::VARCHAR AS id,
        tournament_tag::VARCHAR AS tournament_tag,
        battle_time::TIMESTAMP_NTZ AS battle_time,
        arena_id::INTEGER AS arena_id,
        game_mode_id::INTEGER AS game_mode_id,
        avg_starting_trophies::INTEGER AS avg_starting_trophies
    FROM stg_battles_base
)

SELECT * FROM stg_battles_typed