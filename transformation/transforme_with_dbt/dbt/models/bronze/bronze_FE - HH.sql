-- Modele bronze pour FE - HH
select *
from {{ source('opendatahub', 'FE - HH') }};
