#!/bin/bash

# NAMESPACE="airflow"
# admin_env="admin"
# pg_user="admin"
# pg_pw="postgres"
# NAMESPACE=airlines
# RELEASE_NAME=airflow

kubectl create namespace airlines

kubectl apply -f pv-dags.yaml -n airlines
kubectl apply -f pvc-dags.yaml -n airlines
kubectl apply -f pv-logs.yaml -n airlines
kubectl apply -f pvc-logs.yaml -n airlines

sleep 3

helm install airflow apache-airflow/airflow -f override.yaml \
--namespace airlines \
--set airflow.image.repository=glampe/dst_airlines_custom_airflow \
--set airflow.image.tag=0.1.0 \
--set dags.persistence.enabled=false \
--set dags.gitSync.enabled=true \
--set dags.persistence.existingClaim=airflow-pvc-dags \
--set logs.persistence.enabled=true \
--set logs.persistence.existingClaim=airflow-pvc-logs \
--set airflow.extraAnnotations."prometheus.io/scrape"="true" \
--set airflow.extraAnnotations."prometheus.io/port"="8080"

#Installation : 

# ssh-keygen -t rsa -b 4096 -C "remiducroc@gmail.com"
# base64 airflow -w 0 > temp.txt
# cat temp.txt
# cat airflow.pub

# kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airlines


# helm install airflow apache-airflow/airflow \
# --namespace airlines \
# --set airflow.image.repository=glampe/dst_airlines_custom_airflow \
# --set airflow.image.tag=0.1.0 
# --set webserver.defaultUser.username="$admin_env" \
# --set webserver.defaultUser.password="$admin_env" \
# --set postgresql.auth.postgresPassword="$pg_pw" \
# --set postgresql.auth.username="$pg_user" \
# --set postgresql.auth.password="$pg_pw"





# helm upgrade --install airflow apache-airflow/airflow -f override.yaml \
# --namespace airlines \
# --set dags.persistence.enabled=false \
# --set dags.gitSync.enabled=true \
# --set dags.persistence.existingClaim=airflow-pvc-dags \
# --set logs.persistence.enabled=true \
# --set logs.persistence.existingClaim=airflow-pvc-logs \
# --set airflow.extraAnnotations."prometheus.io/scrape"="true" \
# --set airflow.extraAnnotations."prometheus.io/port"="8080"



#  helm upgrade --install $RELEASE_NAME apache-airflow/airflow -f override.yaml --namespace $NAMESPACE --set dags.persistence.enabled=false --set dags.gitSync.enabled=true

