WITH source AS (
    SELECT
        *
    FROM {{ source('match_info', 'wincons') }}
)

SELECT
CARD_ID::INTEGER AS CARD_ID,
CARD_NAME::VARCHAR AS CARD_NAME,
LOAD_DATE
FROM source