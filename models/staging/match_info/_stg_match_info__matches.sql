{{ config(
    materialized='view'
) }}

WITH source AS (
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

transformed AS (
    SELECT
        id::VARCHAR AS id,
        NULLIF(tournament_tag, 'NULL')::VARCHAR AS tournament_tag,
        battle_time::TIMESTAMP_NTZ AS battle_time,
        arena_id::INTEGER AS arena_id,
        game_mode_id::INTEGER AS game_mode_id,
        avg_starting_trophies::INTEGER AS avg_starting_trophies
    FROM source
)

SELECT * FROM transformed