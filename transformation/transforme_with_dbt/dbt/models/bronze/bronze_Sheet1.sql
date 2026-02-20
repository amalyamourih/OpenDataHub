-- Modèle bronze pour Sheet1
select *
from {{ source('opendatahub', 'Sheet1') }};
