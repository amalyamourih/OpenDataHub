-- Modèle bronze pour FM - FF
select *
from {{ source('opendatahub', 'FM - FF') }};
