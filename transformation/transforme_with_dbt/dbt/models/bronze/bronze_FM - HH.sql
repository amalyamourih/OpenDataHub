-- Modele bronze pour FM - HH
select *
from {{ source('opendatahub', 'FM - HH') }};
