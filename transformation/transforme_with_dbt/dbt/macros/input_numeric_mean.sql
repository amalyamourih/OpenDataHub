{% macro impute_numeric_mean(relation) %}

{% set cols = adapter.get_columns_in_relation(relation) %}

SELECT
{% for col in cols %}
    {% set name = col.name %}
    {% set type = col.data_type | upper %}

    {% if type in ['INTEGER', 'BIGINT', 'SMALLINT', 'FLOAT', 'DOUBLE', 'REAL'] %}
        -- remplace les valeurs NULL par la moyenne
        COALESCE({{ name }}, (SELECT AVG({{ name }}) FROM {{ relation }})) AS {{ name }}

    {% else %}
        {{ name }}  -- colonnes non numériques
    {% endif %}

    {% if not loop.last %},{% endif %}
{% endfor %}

FROM {{ relation }}

{% endmacro %}
