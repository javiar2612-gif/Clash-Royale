{{ config(materialized='table') }}

SELECT
    CARD_ID,
    CARD_NAME,
    CARD_TYPE,
    COST,
    IS_WINCON,
    CONCAT(
        'https://royaleapi.github.io/cr-api-assets/cards/',
        REPLACE(LOWER(CARD_NAME), ' ', '-'), -- Transforma a minúsculas y reemplaza espacios por guiones
        '.png'
    ) AS CARD_IMAGE_URL
FROM {{ ref("_stg_match_info__cards") }}