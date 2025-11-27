{{ config(
    materialized='incremental',
    unique_key='clan_tag', 
    on_schema_change='fail'
) }}

WITH all_clans_history AS (
    {{ unpivot_match_info(
        column_list=[
            'battleTime',     
            'clan_tag',       
            'clan_badgeid',
            'load_date'
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
        load_date,
        ROW_NUMBER() OVER (
            PARTITION BY clan_tag 
            ORDER BY battleTime DESC
        ) AS rn
    FROM all_clans_history
    WHERE clan_tag IS NOT NULL 
        AND clan_tag != 'NULL'

    {% if is_incremental() %}
    AND load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
)

SELECT
    clan_tag::VARCHAR AS clan_tag,
    clan_badgeid::VARCHAR AS clan_badge_id, 
    battleTime::TIMESTAMP_NTZ AS last_seen_battle_time,
    load_date::TIMESTAMP_NTZ AS load_date
FROM deduplicated_clans
WHERE rn = 1