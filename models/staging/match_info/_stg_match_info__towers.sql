{{ config(
    materialized='incremental',
    unique_key='unique_id', 
    on_schema_change='fail'
) }}

WITH base AS (

    {{ unpivot_match_info(
        column_list=[
            'id',
            'tag',
            'kingTowerHitPoints',
            'princessTowersHitPoints',
            'load_date'
        ],
        participant_fields=[
            'tag',
            'kingTowerHitPoints',
            'princessTowersHitPoints'
        ]
    ) }}

),

clean AS (
    SELECT
        id::VARCHAR AS battle_id,
        tag::VARCHAR AS player_tag,
        load_date,

        TRY_TO_NUMBER(REGEXP_REPLACE(kingTowerHitPoints, '\\[|\\]', '')) AS king_hp,

        SPLIT(REGEXP_REPLACE(princessTowersHitPoints, '\\[|\\]', ''), ',') AS princess_array
    FROM base

    {% if is_incremental() %}
    WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
),

princess AS (
    SELECT
        battle_id,
        player_tag,

        CASE
            WHEN INDEX = 0 THEN 'princess_left'
            WHEN INDEX = 1 THEN 'princess_right'
        END AS tower_type,

        TRY_TO_NUMBER(value::STRING) AS hitpoints,
        load_date,
        SHA1(battle_id || player_tag || tower_type) AS unique_id 
    FROM clean,
    LATERAL FLATTEN(input => princess_array)
),

king AS (
    SELECT
        battle_id,
        player_tag,
        'king' AS tower_type,
        king_hp AS hitpoints,
        load_date,
        SHA1(battle_id || player_tag || 'king') AS unique_id 
    FROM clean
)

SELECT 
    unique_id::VARCHAR AS unique_id,
    battle_id::VARCHAR AS battle_id,
    player_tag::VARCHAR AS player_tag,
    tower_type::VARCHAR AS tower_type,
    hitpoints::INT AS hitpoints,
    load_date::TIMESTAMP_NTZ AS load_date
FROM king
UNION ALL
SELECT 
    unique_id::VARCHAR AS unique_id,
    battle_id::VARCHAR AS battle_id,
    player_tag::VARCHAR AS player_tag,
    tower_type::VARCHAR AS tower_type,
    hitpoints::INT AS hitpoints,
    load_date::TIMESTAMP_NTZ AS load_date
FROM princess