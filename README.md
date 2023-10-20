# Coderhouse - Data Engineering

Proyecto de data engineering

# Folders

  - App: Scripts python/sql para extracción, transformación y carga de datos.
  - Ejercicios: Script con ejercicios de clase.

# API

  - [Balldontlie](https://app.balldontlie.io/): Api pública con datos de la NBA.

# Language

  - Python: 3.11.4

# Database

  - Amazon Redshift

# Run Script

  - pip3 install -r App/requirements.txt
  - python3 App/dags/etl_nba/main.py

# Airflow-Docker

  - cd App
  - docker-compose up airflow-init
  - docker-compose up
  - localhost:8080 -> Dag: dag_etl_nba

