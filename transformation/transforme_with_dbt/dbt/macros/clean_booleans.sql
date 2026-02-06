{% macro clean_booleans(col) %}
    TRY_CAST({{ col }} AS BOOLEAN)
{% endmacro %}
