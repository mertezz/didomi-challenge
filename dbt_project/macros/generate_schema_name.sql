-- Example of overriding dbt functionality  
-- This overrides the generate_schema_name macro  
-- NOTE: Be careful – this affects all developers and environments

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}
    
        {{ custom_schema_name | trim }} 

    {%- endif -%}

{%- endmacro %}