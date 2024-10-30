# DST-Airlines

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

Predict airplane flight delays based on data collected from [Lufthansa API](https://developer.lufthansa.com/page), weather forecasts from [Open Meteo](https://open-meteo.com/) and external airport data.

## Project Organization

Main folder organisation of the project

```
├── kubernetes                  # Kubernetes configuration files for deploying the application
│   ├── fastapi                 # Kubernetes manifests for the FastAPI application
│   │   └── templates           # Templates for Kubernetes FastAPI deployments and configurations
│   ├── mysql                   # Kubernetes manifests for the MySQL database service
│   │   └── templates           # Templates for MySQL Kubernetes deployments and configurations
│   ├── airflow                 # Kubernetes manifests for deploying Apache Airflow
│   ├── mongo-DB                # Kubernetes manifests for the MongoDB database service
│   │   └── templates           # Templates for MongoDB Kubernetes deployments and configurations
│   └── prometheus_grafana      # Kubernetes manifests for Prometheus and Grafana monitoring tools
├── fastapi                     # FastAPI application source code and configurations
│   └── app                     # FastAPI application source files
├── notebooks                   # Jupyter notebooks for data exploration and analysis
├── databases_compose           # Docker Compose configuration for local database services
├── .github                     # GitHub configuration for CI/CD workflows
│   ├── workflows               # GitHub Actions workflows for CI/CD automation
│   └── archive                 # Archive for old or unused GitHub workflows
├── logs                        # Directory for storing application and system logs
├── env                         # Environment configuration files for different deployment stages
├── airflow                     # Configuration and data for the Airflow ETL pipeline
│   ├── config                  # Airflow configuration files
│   ├── logs                    # Airflow log files
│   ├── plugins                 # Custom Airflow plugins for ETL workflows
│   ├── dags                    # Directory containing Airflow DAGs (Directed Acyclic Graphs)
│   ├── raw_files               # Raw data files used in Airflow ETL processing
│   └── clean_data              # Directory for storing processed and cleaned data files
├── dst_airlines                # Internal project package for data processing and modeling
│   ├── modeling                # Machine learning and statistical modeling modules
│   ├── logging                 # Logging configurations and utilities for the project
│   ├── data                    # Data handling and manipulation modules
│   └── database                # Database connection and interaction modules
├── crontab_scripts             # Scripts scheduled by cron for automated tasks
└── data                        # Project data storage directory with various data stages
    ├── 1_raw                   # Raw, original, and unprocessed data
    ├── 2_interim               # Intermediate data files, transformed but not final
    ├── 3_processed             # Fully processed and ready-to-use data files
    └── 4_external              # Data from third-party or external sources

```

--------

## Objectives

This project proposes an application predicting in real-time potential delay of fights taking of from Frankfurt airport.

It was developed as the final project of my DataScientest Bootcamp.

It has the following infrastructure:
- Deployed on Kubernetes using custom Docker images
- Exposed via FastAPI
- Orchestrated by Airflow, enabling to: 
  - collect and store data from APIs
  - train three ML models and select the best performing one
  - make prediction when called internally by the FastAPI "front"
- Processing data via an in-house Python package, installed on the Airflow worker image
- Storing data into a:
  - MongoDB database (for raw, unstructured flight data)
  - MySQL database (for structured flight, weather forecast and airport data)

Security was as much as possible taken into consideration by:
- Deploying a OAuth2.0 authentication mechanism on FastAPI
- Securing secrets while still sharing template to ease appropriation by ensuring their are not synched on GitHub and by relying on GitGuardian

## Project deployment

To deploy the project:
1. Use the provided templates to generate the corresponding files - ensure coherence between the override.yaml and the secret.yaml: 
   1. ```override_template.yaml``` in ```kubernetes/airflow```
   2. ```secret_template.yaml``` in ```kubernetes/mongodb or mysql``` (```secret.yaml``` are to be stored in the corresponding ```template``` folder)
2. Deploy the Kubernetes infrastructure via the ```kubernetes/deploy.sh``` script
3. Expose the Airflow and the FastAPI ports to enable interaction:
   1. Airflow: ```kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airlines```
   2. FastAPI: ```kubectl port-forward svc/fastapi-service 8000:80 -n airlines```
4. Launch the DAGs in the following order: 
   1. structure airport data (natively stored into the Docker image - it is not ideal as it would be preferable to collect them from a official source)
   2. collect from API and structure flight and weather forecasts data (be sure to have created a key on the Lufthansa API (see link above) and verify it's working)
   3. train model on structured data
5. Request the API to get a prediction for a flight departing from Frankfurt (note that the Lufthansa public API only expose data for the coming week)

The infrastructure can be turned down with the ```kubernetes/uninstall.sh``` script