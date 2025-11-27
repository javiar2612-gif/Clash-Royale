-- tests/assert_avg_elixir_cost_is_reasonable.sql
SELECT
    *
FROM
    {{ ref('base_match_info__cards') }}
WHERE
    cost < 1.0 OR cost > 10.0