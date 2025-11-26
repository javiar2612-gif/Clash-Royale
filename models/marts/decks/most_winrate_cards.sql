WITH card_stats AS (
    SELECT
        ca.cost,
        ca.card_id,
        ca.card_name,

        -- Métricas básicas
        COUNT(*) AS total_plays,
        SUM(CASE WHEN b.outcome = 'winner' THEN 1 ELSE 0 END) AS wins,
        COUNT(*) - SUM(CASE WHEN b.outcome = 'winner' THEN 1 ELSE 0 END) AS losses,

        -- Win Rate (%)
        (SUM(CASE WHEN b.outcome = 'winner' THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(*), 0) AS winrate,

        -- Usage Rate (%)
        -- Fórmula: (Mis apariciones / (Total de cartas jugadas / 8 huecos)) * 100
        (COUNT(*) * 100.0) / (SUM(COUNT(*)) OVER() / 8) AS usage_rate

    FROM {{ ref('fct_battle_decks') }} b
    LEFT JOIN {{ ref('dim_card') }} ca
        ON ca.card_id = b.card_id
    GROUP BY 1, 2, 3
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cost ORDER BY winrate DESC) AS rn
    FROM card_stats
)

SELECT
    card_name,
    cost,
    ROUND(winrate, 2) as winrate_pct,
    ROUND(usage_rate, 2) as usage_rate_pct
FROM ranked
WHERE rn = 1
ORDER BY cost ASC