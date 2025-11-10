-- Common constants, functions & descriptions

{% macro run_id() %}
    '{{ invocation_id }}'
{% endmacro %}


-- Default values (defined as SQL strings, used in all facts and dimensions)

{% macro default_string() %}
    'n/a'
{% endmacro %}

{% macro default_int64() %}
    0
{% endmacro %}

{% macro default_timestamp_minimum() %}
    '1900-01-01'::timestamp
{% endmacro %}

{% macro default_timestamp_maximum() %}
    '9999-12-31'::timestamp
{% endmacro %}

{% macro default_json() %}
    '{}'
{% endmacro %}

{% macro default_date() %}
    '1900-01-01'::date
{% endmacro %}

{% macro default_currency() %}
    'EUR'
{% endmacro %}
