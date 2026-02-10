{% macro deduplicate(relation, pk) %}

SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY {{ pk }} ORDER BY {{ pk }}) AS rn
    FROM {{ relation }}
)
WHERE rn = 1

{% endmacro %}
