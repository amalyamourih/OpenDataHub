-- Modele bronze pour cog_france_ccom
select *
from {{ source('opendatahub', 'cog_france_ccom') }};
