kubectl create namespace airlines
helm install mongodb ./mongo-DB/ -n airlines
helm install mysql ./mysql/ -n airlines
helm install fastapi ./fastapi/ -n airlines
bash ./airflow/AirflowSync.sh


# Post Forwarding du webserver

#kubectl port-forward svc/fastapi-service 8000:80 -n airlines

#kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airlines

#Avoir l'ip cluster
#kubectl get nodes -o wide

# Nécessité d'avoir une storageClass de type local-path, pour le faire sur minikube (https://minikube.sigs.k8s.io/docs/tutorials/local_path_provisioner/) :
# minikube addons enable storage-provisioner-rancher