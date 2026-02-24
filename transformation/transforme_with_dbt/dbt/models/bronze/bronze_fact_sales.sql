-- ModeZle bronze pour fact_sales
select *
from {{ source('opendatahub', 'fact_sales') }};
                        