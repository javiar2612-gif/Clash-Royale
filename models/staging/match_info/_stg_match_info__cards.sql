WITH
stg_cards AS (
    SELECT
        *
    FROM {{ ref("base_match_info__cards") }}
),
stg_wincons AS (
    SELECT
        CARD_ID,
    FROM {{ ref("base_match_info__wincons") }}
)

SELECT
    c.CARD_ID,
    c.CARD_NAME,
    c.CARD_TYPE,
    c.COST,

    CASE
        WHEN w.CARD_ID IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_wincon
    
FROM
    stg_cards AS c
LEFT JOIN
    stg_wincons AS w
    ON c.CARD_ID = w.CARD_ID