-- Modele bronze pour FM - HH-FF
select *
from {{ source('opendatahub', 'FM - HH-FF') }};
