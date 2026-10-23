
# Airflow Weather ETL Pipeline

## About
An automated ETL pipeline that extracts weather data from the Open-Meteo API, transforms it using Python, and loads it into PostgreSQL using Apache Airflow.

## Technologies
- Python
- Apache Airflow
- PostgreSQL
- Open-Meteo API
- Docker
- Git and GitHub

## Workflow
1. Extract weather data from the API.
2. Transform the required weather fields.
3. Load the data into PostgreSQL.
4. Schedule and monitor the pipeline using Airflow.

## How to Run
1. Install Docker Desktop.
2. Open a terminal in the project directory.
3. Run `docker compose up -d`.
4. Open the Airflow web interface at `http://localhost:8080` if configured on the default port.
5. Configure the required API and PostgreSQL connections.

## Author
Vijay
