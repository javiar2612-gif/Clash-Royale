{{ config(
    materialized='view'
) }}

WITH unpivoted_participants AS (

    {{ unpivot_match_info(
        column_list=[
            'id',
            'tag',
            'startingTrophies',
            'trophyChange',
            'crowns',
        ],
        participant_fields=[
            'tag',
            'startingTrophies',
            'trophyChange',
            'crowns',
        ]
    ) }}
),

transformed AS (
    SELECT
        id::VARCHAR AS battle_id,
        tag::VARCHAR AS player_tag,
        outcome::VARCHAR AS outcome,
        startingTrophies::INTEGER AS starting_trophies,
        trophyChange::INTEGER AS trophy_change,
        crowns::INTEGER AS crowns,
             
    FROM unpivoted_participants
)

SELECT * FROM transformed