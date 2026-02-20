-- Modèle bronze pour FM - HF
select *
from {{ source('opendatahub', 'FM - HF') }};
