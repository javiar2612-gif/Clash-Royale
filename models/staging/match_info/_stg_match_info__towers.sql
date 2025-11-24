{{ config(
    materialized='view'
) }}

WITH base AS (

    {{ unpivot_match_info(
        column_list=[
            'id',
            'tag',
            'kingTowerHitPoints',
            'princessTowersHitPoints'
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

        TRY_TO_NUMBER(REGEXP_REPLACE(kingTowerHitPoints, '\\[|\\]', '')) AS king_hp,

        SPLIT(REGEXP_REPLACE(princessTowersHitPoints, '\\[|\\]', ''), ',') AS princess_array
    FROM base
),

princess AS (
    SELECT
        battle_id,
        player_tag,

        CASE
            WHEN INDEX = 0 THEN 'princess_left'
            WHEN INDEX = 1 THEN 'princess_right'
        END AS tower_type,

        TRY_TO_NUMBER(value::STRING) AS hitpoints   -- 🔥 FIX AQUI
    FROM clean,
    LATERAL FLATTEN(input => princess_array)
),

king AS (
    SELECT
        battle_id,
        player_tag,
        'king' AS tower_type,
        king_hp AS hitpoints
    FROM clean
)

SELECT * FROM king
UNION ALL
SELECT * FROM princess
