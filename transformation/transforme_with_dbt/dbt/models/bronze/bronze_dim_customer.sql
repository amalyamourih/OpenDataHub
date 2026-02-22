-- Modele bronze pour dim_customer
select *
from {{ source('opendatahub', 'dim_customer') }};