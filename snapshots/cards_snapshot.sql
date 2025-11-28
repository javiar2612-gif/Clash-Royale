{% snapshot cards_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='CARD_ID',
        strategy='check',
        check_cols=['CARD_NAME', 'CARD_TYPE', 'COST']
    )
}}

SELECT
    CARD_ID,
    CARD_NAME,
    CARD_TYPE,
    COST,
    LOAD_DATE
FROM {{ ref('base_match_info__cards') }}

{% endsnapshot %}
