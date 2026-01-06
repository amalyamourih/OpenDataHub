{% macro clean_strings(col) %}
    NULLIF(LOWER(TRIM({{ col }})), '')
{% endmacro %}
