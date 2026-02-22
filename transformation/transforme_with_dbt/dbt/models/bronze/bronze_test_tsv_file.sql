-- Modele bronze pour test_tsv_file
select *
from {{ source('opendatahub', 'test_tsv_file') }};
