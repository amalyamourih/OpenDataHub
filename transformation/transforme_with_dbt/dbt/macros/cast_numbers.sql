{% macro cast_numbers(col, data_type) %}
    TRY_CAST({{ col }} AS {{ data_type }})
{% endmacro %}
