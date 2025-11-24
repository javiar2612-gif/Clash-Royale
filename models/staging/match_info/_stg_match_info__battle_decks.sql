{{ config(
    materialized='view'
) }}

WITH all_decks_unpivoted AS (

    {{ unpivot_deck_cards() }}
)

SELECT
    battle_id::VARCHAR AS battle_id,
    player_tag::VARCHAR AS player_tag,
    card_id::INTEGER AS card_id,
    card_level::INTEGER AS level
FROM all_decks_unpivoted
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY battle_id, player_tag, card_id 
    ORDER BY 1
) = 1