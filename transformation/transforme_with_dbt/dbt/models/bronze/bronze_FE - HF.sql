-- Modèle bronze pour FE - HF
select *
from {{ source('opendatahub', 'FE - HF') }};
