-- Modele bronze pour dim_store
select *
from {{ source('opendatahub', 'dim_store') }};
                        