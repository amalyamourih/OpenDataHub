-- Modele bronze pour FE - HH-FF
select *
from {{ source('opendatahub', 'FE - HH-FF') }};
