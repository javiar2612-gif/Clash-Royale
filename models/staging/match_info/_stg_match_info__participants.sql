{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}


WITH unpivoted_participants AS (

    {{ unpivot_match_info(
        column_list=[
            'id',
            'tag',
            'battleTime',
            'startingTrophies',
            'trophyChange',
            'crowns',
            'load_date'
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
        {{ dbt_utils.generate_surrogate_key(['id','tag']) }} AS id,
        {{ dbt_utils.generate_surrogate_key(['id','battleTime']) }} AS battle_id,
        tag::VARCHAR AS player_tag,
        outcome::VARCHAR AS outcome,
        startingTrophies::INTEGER AS starting_trophies,
        trophyChange::INTEGER AS trophy_change,
        crowns::INTEGER AS crowns,
        load_date
             
    FROM unpivoted_participants
    {% if is_incremental() %}
    WHERE load_date > (SELECT MAX(t.load_date) FROM {{ this }} AS t)
    {% endif %}
)

SELECT * FROM transformed