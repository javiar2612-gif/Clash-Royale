WITH source AS (
    SELECT
        *
    FROM {{ source('match_info', 'cards') }}
)

SELECT
CARD_ID::INTEGER AS CARD_ID,
CARD_NAME::VARCHAR AS CARD_NAME,
TYPE:: VARCHAR AS CARD_TYPE,
COST:: INTEGER AS COST,
LOAD_DATE
FROM source