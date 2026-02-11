import os 

# Recuperer la nom de la table a partir du clé de l'objet 
def get_table_name_from_key(key):
    filename = os.path.basename(key)
    table_name = os.path.splitext(filename)[0]
    return table_name