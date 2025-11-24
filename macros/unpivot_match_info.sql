{% macro unpivot_match_info(column_list, participant_fields) %}
    
    {% set prefixes = ['winner', 'loser'] %}
    {% set union_statements = [] %}

    {% for prefix in prefixes %}
        
        {% set select_fields = [] %}
        
        {% for column in column_list %}
            
            {% if column in participant_fields %}
                {% set prefixed_column = prefix ~ '_' ~ column %}
                
                {% do select_fields.append(prefixed_column ~ ' AS ' ~ column) %}
            
            {% else %}
                {% do select_fields.append(column) %}
            {% endif %}

        {% endfor %}

        {% do select_fields.append("'" ~ prefix ~ "' AS outcome") %}
        
        {% set sql_block %}
            SELECT
                {{ select_fields | join(',\n\t\t') }}
            FROM {{ source('match_info', 'match_data') }}
            WHERE {{ prefix }}_tag IS NOT NULL
        {% endset %}

        {% do union_statements.append(sql_block) %}

    {% endfor %}

    {{ union_statements | join('\n\n\tUNION ALL\n\n') }}

{% endmacro %}