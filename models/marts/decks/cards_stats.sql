WITH card_stats AS (
    SELECT
        ca.card_id,
        ca.card_name,
        ca.cost,

        COUNT(*) AS total_plays,
        
        (SUM(CASE WHEN b.outcome = 'winner' THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(*), 0) AS winrate,

        (COUNT(*) * 100.0) / (SUM(COUNT(*)) OVER() / 8) AS usage_rate

    FROM {{ ref('fct_battle_decks') }} b
    LEFT JOIN {{ ref('dim_card') }} ca
        ON ca.card_id = b.card_id
        WHERE b.starting_trophies >=6000
    GROUP BY 1, 2, 3
),

ranked_cards AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_plays DESC) as rank_most_used,
        ROW_NUMBER() OVER (ORDER BY total_plays ASC) as rank_least_used
    FROM card_stats
)

SELECT
    rank_most_used as ranking_global,
    card_name,
    cost,
    ROUND(usage_rate, 2) as usage_rate_pct,
    ROUND(winrate, 2) as winrate_pct
FROM ranked_cards
WHERE rank_most_used <= 10  
   OR rank_least_used <= 10 
ORDER BY ranking_global DESC