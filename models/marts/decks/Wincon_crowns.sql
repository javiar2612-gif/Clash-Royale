WITH wincon_data AS (
    SELECT
        b.card_id,
        ca.card_name,
        b.outcome,
        b.crowns,
        ca.is_wincon

    FROM {{ ref('fct_battle_decks') }} b
    INNER JOIN {{ ref('dim_card') }} ca
        ON ca.card_id = b.card_id
    
    WHERE b.outcome = 'winner' 
      AND ca.is_wincon = TRUE
      AND b.starting_trophies >= 5000
)

SELECT
    card_name,
    AVG(crowns)::NUMERIC(10, 2) AS avg_crowns_on_win
FROM wincon_data
GROUP BY 1
ORDER BY avg_crowns_on_win DESC