{{ config(
    materialized='incremental',
    unique_key='tag', 
    on_schema_change='fail'
) }}

WITH player_data_raw AS (
    {{ unpivot_match_info(
        column_list=[
            'id',
            'battleTime',
            'tag',
            'startingTrophies',
            'clan_tag',
            'load_date'
        ],
        participant_fields=[
            'tag',
            'clan_tag',
            'startingTrophies'
        ]
    ) }}
),

latest_player_info AS (
    SELECT
        tag,
        clan_tag,
        startingTrophies,
        battleTime,
        load_date,
        
        ROW_NUMBER() OVER (
            PARTITION BY tag 
            ORDER BY battleTime DESC
        ) AS rn
    FROM player_data_raw
    
    {% if is_incremental() %}
    WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
)

SELECT
    tag::VARCHAR AS tag,
    NULLIF(clan_tag, 'NULL')::VARCHAR AS current_clan_tag,
    startingTrophies::INT AS latest_starting_trophies,
    load_date::TIMESTAMP_NTZ AS load_date

FROM latest_player_info
WHERE rn = 1