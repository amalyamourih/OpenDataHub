{% macro normalize_nulls(col) %}
    NULLIF({{ col }}, 'null')
{% endmacro %}
