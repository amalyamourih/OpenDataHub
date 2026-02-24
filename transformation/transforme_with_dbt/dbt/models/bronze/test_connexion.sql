SELECT 
    current_version() as snowflake_version,
    current_warehouse() as warehouse_used,
    'Connexion réussie !' as statut