-- Modele bronze pour tournee
select *
from {{ source('opendatahub', 'tournee') }};
