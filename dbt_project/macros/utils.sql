{% macro incremental_filter(column_name, start_date, end_date) %}
    {% if is_incremental() %}
        where {{ column_name }} >= {{ safe_date(start_date) }}
          and {{ column_name }} <= {{ safe_date(end_date) }}
    {% endif %}
{% endmacro %}

{% macro safe_date(value) %}
    {# If the value contains a space or parentheses, treat it as a SQL expression #}
    {% if ' ' in value or '(' in value %}
        {{ value }}
    {% else %}
        TO_DATE('{{ value }}', 'YYYY-MM-DD')
    {% endif %}
{% endmacro %}