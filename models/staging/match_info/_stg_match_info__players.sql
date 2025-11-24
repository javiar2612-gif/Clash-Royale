{{ config(
    materialized='view'
) }}

WITH player_data_raw AS (
    {{ unpivot_match_info(
        column_list=[
            'id',             
            'battleTime',     
            'tag',            
            'clan_tag'        
        ],
        participant_fields=[
            'tag',
            'clan_tag'
        ]
    ) }}
),

latest_player_info AS (
    SELECT
        tag,
        clan_tag,
        battleTime,
        
        ROW_NUMBER() OVER (
            PARTITION BY tag 
            ORDER BY battleTime DESC
        ) AS rn
    FROM player_data_raw
)

SELECT
    tag::VARCHAR AS tag,                        
    NULLIF(clan_tag, 'NULL')::VARCHAR AS current_clan_tag

FROM latest_player_info
WHERE rn = 1