{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id','battletime']) }} AS id,
        s.tournamentTag AS tournament_tag,
        s.battleTime AS battle_time,
        s.arena_id,
        s.gameMode_id AS game_mode_id,
        s.average_startingTrophies AS avg_starting_trophies,
        s.load_date
    FROM 
        {{ source('match_info','match_data') }} AS s
    
    {% if is_incremental() %}
    WHERE s.load_date > (SELECT MAX(t.load_date) FROM {{ this }} AS t)
    {% endif %}
),

transformed AS (
    SELECT
        id::VARCHAR AS id,
        NULLIF(tournament_tag, 'NULL')::VARCHAR AS tournament_tag,
        battle_time::TIMESTAMP_NTZ AS battle_time,
        arena_id::INTEGER AS arena_id,
        game_mode_id::INTEGER AS game_mode_id,
        avg_starting_trophies::INTEGER AS avg_starting_trophies,
        load_date
    FROM source
)

SELECT * FROM transformed