#!/bin/bash

echo " -- Start : Add and update apache airflow -- "
helm repo add apache-airflow https://airflow.apache.org
helm repo update
echo " -- End : Create namespace airlines -- "

echo " -- Start : Create namespace airlines -- "
kubectl create namespace airlines
echo " -- End : Create namespace airlines -- "

echo " -- Start : Apply PV / PVC -- "
kubectl apply -f ./airflow/pv-logs.yaml -n airlines
kubectl apply -f ./airflow/pvc-logs.yaml -n airlines
echo " -- End : Apply PV / PVC -- "

echo " -- Start : Sleep 3s -- "
sleep 3
echo " -- End : Sleep 3s -- "

echo " -- Start : Install Airflow -- "
helm upgrade --install airflow apache-airflow/airflow -f ./airflow/override.yaml --namespace airlines
echo " -- End : Install Airflow -- "

# Pour port-forward le service, exécutez la commande suivante :

# kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airlines


# Installation of GitSync: 

## Generation of the key:
# ssh-keygen -t rsa -b 4096 -C "GitSync for Airflow"

## Encrypting of the private key in base64
# base64 key.private -w 0 > temp.txt

## Get the base64 key
# cat temp.txt