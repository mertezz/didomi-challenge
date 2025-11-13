{% test is_positive(model, column_name) %}

select {{ column_name }} as val
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
