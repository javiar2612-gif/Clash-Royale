{{ config(
    materialized='view'
) }}

WITH unpivoted_participants AS (

    {{ unpivot_match_info(
        column_list=[
            'id',
            'tag',
            'clan_tag',
            'startingTrophies',
            'trophyChange',
            'crowns',
            'kingTowerHitPoints',
            'princessTowersHitPoints'
        ],
        participant_fields=[
            'tag',
            'clan_tag',
            'startingTrophies',
            'trophyChange',
            'crowns',
            'kingTowerHitPoints',
            'princessTowersHitPoints'
        ]
    ) }}
),

transformed AS (
    SELECT
        id::VARCHAR AS battle_id,
        tag::VARCHAR AS player_tag,
        clan_tag::VARCHAR AS clan_tag_at_battle,
        outcome::VARCHAR AS outcome,
        startingTrophies::INTEGER AS starting_trophies,
        trophyChange::INTEGER AS trophy_change,
        crowns::INTEGER AS crowns,
        
        TRY_TO_NUMBER(REGEXP_REPLACE(kingTowerHitPoints, '\\[|\\]', ''))::INTEGER AS king_tower_hp,
        REGEXP_REPLACE(princessTowersHitPoints, '\\[|\\]', '')::VARCHAR AS princess_towers_hp
        
    FROM unpivoted_participants
)

SELECT * FROM transformed