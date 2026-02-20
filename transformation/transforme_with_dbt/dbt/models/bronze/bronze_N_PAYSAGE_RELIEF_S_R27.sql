-- Modèle bronze pour N_PAYSAGE_RELIEF_S_R27
select *
from {{ source('opendatahub', 'N_PAYSAGE_RELIEF_S_R27') }};
