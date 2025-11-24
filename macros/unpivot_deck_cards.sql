{% macro unpivot_deck_cards() %}
    
    {% set prefixes = ['winner', 'loser'] %}
    {% set union_statements = [] %}

    {% for prefix in prefixes %}
        {% for i in range(1, 9) %}
            
            {% set select_fields = [] %}
            
            {% do select_fields.append('id AS battle_id') %}
            {% do select_fields.append(prefix ~ '_tag AS player_tag') %}
            
            {% do select_fields.append(prefix ~ '_card' ~ i ~ '_id AS card_id') %}
            {% do select_fields.append(prefix ~ '_card' ~ i ~ '_level AS card_level') %}
            
            {% set sql_block %}
                SELECT
                    {{ select_fields | join(',\n\t\t') }}
                FROM {{ source('match_info','match_data') }}
                WHERE {{ prefix ~ '_card' ~ i ~ '_id' }} IS NOT NULL
            {% endset %}

            {% do union_statements.append(sql_block) %}

        {% endfor %}
    {% endfor %}

    {{ union_statements | join('\n\n\tUNION ALL\n\n') }}

{% endmacro %}