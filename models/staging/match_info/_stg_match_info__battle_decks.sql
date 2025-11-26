{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}

WITH all_decks_unpivoted AS (

    {{ unpivot_deck_cards() }}
),

transformed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id','player_tag','card_id']) }} AS id,
        {{ dbt_utils.generate_surrogate_key(['id','battleTime']) }} AS battle_id,
        player_tag::VARCHAR AS player_tag,
        card_id::INTEGER AS card_id,
        card_level::INTEGER AS level,
        load_date
    FROM all_decks_unpivoted

    {% if is_incremental() %}
    WHERE load_date > (SELECT MAX(t.load_date) FROM {{ this }} AS t)
    {% endif %}
)

SELECT * FROM transformed
