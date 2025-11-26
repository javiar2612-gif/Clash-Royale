{{ config(materialized='table') }}

SELECT
    CARD_ID,
    CARD_NAME,
    CARD_TYPE,
    COST,
    IS_WINCON
FROM {{ ref("_stg_match_info__cards") }}