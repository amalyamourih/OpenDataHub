-- Modèle bronze pour fact_returns
                            select *
                            from { source('opendatahub', 'fact_returns') };
                        