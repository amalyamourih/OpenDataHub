{% macro drop_technical_columns(col_name) %}
    {% if col_name.startswith('_') or col_name in ['created_at_raw', 'updated_at_raw'] %}
        false
    {% else %}
        true
    {% endif %}
{% endmacro %}
