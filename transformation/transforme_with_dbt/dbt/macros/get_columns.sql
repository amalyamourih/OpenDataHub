{% macro get_columns(relation) %}
    {{ return(adapter.get_columns_in_relation(relation)) }}
{% endmacro %}
