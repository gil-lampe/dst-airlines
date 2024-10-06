#!/bin/bash

helm repo add apache-airflow https://airflow.apache.org
helm repo update

kubectl create namespace airlines

kubectl apply -f ./airflow/pv-dags.yaml -n airlines
kubectl apply -f ./airflow/pvc-dags.yaml -n airlines
kubectl apply -f ./airflow/pv-logs.yaml -n airlines
kubectl apply -f ./airflow/pvc-logs.yaml -n airlines

sleep 3

helm upgrade --install airflow apache-airflow/airflow -f ./airflow/override.yaml \
  --namespace airlines \
  --set images.airflow.repository=glampe/dst_airlines_custom_airflow \
  --set images.airflow.tag=0.1.0 \
  --set dags.persistence.enabled=false \
  --set dags.gitSync.enabled=true \
  --set dags.persistence.existingClaim=airflow-pvc-dags \
  --set logs.persistence.enabled=true \
  --set logs.persistence.existingClaim=airflow-pvc-logs \
  --set airflow.extraAnnotations."prometheus.io/scrape"="true" \
  --set airflow.extraAnnotations."prometheus.io/port"="8080"

# Pour port-forward le service, exécutez la commande suivante :

# kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airlines


#Installation : 

# ssh-keygen -t rsa -b 4096 -C "remiducroc@gmail.com"
# base64 key.private -w 0 > temp.txt
# cat temp.txt
# cat key.pub





# helm install airflow apache-airflow/airflow \
# --namespace airlines \
# --set airflow.image.repository=glampe/dst_airlines_custom_airflow \
# --set airflow.image.tag=0.1.0 
# --set webserver.defaultUser.username="$admin_env" \
# --set webserver.defaultUser.password="$admin_env" \
# --set postgresql.auth.postgresPassword="$pg_pw" \
# --set postgresql.auth.username="$pg_user" \
# --set postgresql.auth.password="$pg_pw"
