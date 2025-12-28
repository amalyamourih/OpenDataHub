import paramiko
import getpass

# Informations de connexion
hostname = "82.67.135.54"  # IP de la VM
username = "ter"
port = 31820
 # Remplace 2222 par le port d'écoute SSH réel
password = getpass.getpass("Entrez votre mot de passe SSH : ")

# Connexion SSH à la VM
try:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, port=port, username=username, password=password)
    print("Connexion réussie à la VM !")

    # Exemple : exécuter une commande sur la VM
    stdin, stdout, stderr = ssh.exec_command('uname -a')
    print("Résultat de la commande :", stdout.read().decode())

    ssh.close()
except Exception as e:
    print("Erreur de connexion :", e)