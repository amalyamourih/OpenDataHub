{% macro auto_clean_pipeline(relation) %}

{% set cols = adapter.get_columns_in_relation(relation) %}

SELECT
{% for col in cols %}
    {% set name = col.name %}
    {% set type = col.data_type | upper %}

    {% if type in ['VARCHAR', 'TEXT'] %}
        -- nettoyage des chaînes
        NULLIF(LOWER(TRIM({{ name }})), '') AS {{ name }}

    {% elif type in ['INTEGER', 'BIGINT', 'SMALLINT', 'FLOAT', 'DOUBLE', 'REAL'] %}
        -- cast + imputation moyenne
        COALESCE(
            TRY_CAST({{ name }} AS {{ type }}),
            (SELECT AVG({{ name }}) FROM {{ relation }})
        ) AS {{ name }}

    {% elif type in ['DATE', 'TIMESTAMP'] %}
        -- cast sécurisé pour dates
        TRY_CAST({{ name }} AS {{ type }}) AS {{ name }}

    {% elif type == 'BOOLEAN' %}
        -- cast booléen
        TRY_CAST({{ name }} AS BOOLEAN) AS {{ name }}

    {% else %}
        {{ name }}

    {% endif %}

    {% if not loop.last %},{% endif %}
{% endfor %}

FROM {{ relation }}

{% endmacro %}
