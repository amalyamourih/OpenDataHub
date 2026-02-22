-- Modele bronze pour dim_product
select *
from {{ source('opendatahub', 'dim_product') }};
                        