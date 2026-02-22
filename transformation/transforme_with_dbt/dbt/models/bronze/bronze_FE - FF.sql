-- Modele bronze pour FE - FF
select *
from {{ source('opendatahub', 'FE - FF') }};
