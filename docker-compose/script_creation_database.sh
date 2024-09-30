#! /bin/bash

# Lancement des conteneurs docker-compose
docker-compose up -d

# Initialisation des databases
DB_MYSQL=$(docker ps --filter "ancestor=mysql" --format "{{.Names}}")


docker cp /home/sanou/DST-Airlines/docker-compose/DST_Airlines.sql $DB_MYSQL:/script.sql
echo "Création des utilisateurs."
docker exec -i $DB_MYSQL mysql -u root -ppassword DST_AIRLINES < ./init.sql
echo "Création de la base de données."
docker exec -i $DB_MYSQL mysql -u root -ppassword DST_AIRLINES < ./DST_Airlines.sql



