{% macro incremental_filter(column_name, start_date, end_date) %}
    {% if is_incremental() %}
        where {{ column_name }} >= '{{ start_date }}'
          and {{ column_name }} <= '{{ end_date }}'
    {% endif %}
{% endmacro %}