-- Modèle bronze pour dim_date
                            select *
                            from { source('opendatahub', 'dim_date') };
                        