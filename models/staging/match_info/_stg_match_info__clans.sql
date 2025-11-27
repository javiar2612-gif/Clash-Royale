{{ config(
    materialized='view'
) }}

WITH all_clans_history AS (
    {{ unpivot_match_info(
        column_list=[
            'battleTime',     
            'clan_tag',       
            'clan_badgeid'     
        ],
        participant_fields=[
            'clan_tag',
            'clan_badgeid'
        ]
    ) }}
),

deduplicated_clans AS (
    SELECT
        clan_tag,
        clan_badgeid,
        battleTime,
        ROW_NUMBER() OVER (
            PARTITION BY clan_tag 
            ORDER BY battleTime DESC
        ) AS rn
    FROM all_clans_history
    WHERE clan_tag IS NOT NULL 
      AND clan_tag != 'NULL'
)

SELECT
    clan_tag::VARCHAR AS clan_tag,
    clan_badgeid::VARCHAR AS clan_badge_id, 
    battleTime::TIMESTAMP_NTZ AS last_seen_battle_time
FROM deduplicated_clans
WHERE rn = 1