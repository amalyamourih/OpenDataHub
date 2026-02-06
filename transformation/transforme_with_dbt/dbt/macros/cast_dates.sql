{% macro cast_dates(col, data_type) %}
    TRY_CAST({{ col }} AS {{ data_type }})
{% endmacro %}
