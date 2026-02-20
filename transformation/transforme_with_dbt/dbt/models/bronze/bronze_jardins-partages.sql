-- Modèle bronze pour jardins-partages
select *
from {{ source('opendatahub', 'jardins-partages') }};
