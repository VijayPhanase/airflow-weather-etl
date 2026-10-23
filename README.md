# Airflow Weather ETL Pipeline

## Project Overview
This project is an automated Weather ETL (Extract, Transform, Load) pipeline built using Apache Airflow, Python, and PostgreSQL. It fetches real-time weather data from the Open-Meteo API, processes the data, and stores it in a PostgreSQL database for further analysis.

## Technologies Used
- **Python** – Data extraction and processing
- **Apache Airflow** – Workflow orchestration and scheduling
- **Open-Meteo API** – Weather data source
- **PostgreSQL** – Data storage
- **Docker** – Running the project services
- **Git and GitHub** – Version control

## Project Workflow
1. Extract weather data from the Open-Meteo API.
2. Transform the API response into structured data.
3. Load the processed weather data into PostgreSQL.
4. Use Apache Airflow to schedule and monitor the pipeline.

## Data Collected
- Latitude and longitude
- Date and time
- Temperature
- Relative humidity
- Apparent temperature
- Precipitation
- Wind speed
- Weather code
- Day or night indicator

## Project Structure

```text
airflow-weather-etl/
├── dags/
│   └── weather_etl.py
├── tests/
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```

*Note: The structure above is an example. Adjust the filenames to match your actual project.*

## How to Run the Project

### Prerequisites
- Python
- Docker Desktop
- Git

### Steps

1. Clone the repository:

   ```bash
   git clone https://github.com/VijayPhanase/airflow-weather-etl.git
   ```

2. Open the project folder:

   ```bash
   cd airflow-weather-etl
   ```

3. Start the Airflow services:

   ```bash
   docker compose up -d
   ```

4. Check running containers:

   ```bash
   docker compose ps
   ```

5. Open the Airflow web interface in your browser using the port configured in your Docker Compose file, commonly:

   `http://localhost:8080`

6. Configure the required API and PostgreSQL connections in Airflow, then trigger the weather ETL DAG.

## Learning Outcomes
- Understanding ETL pipeline development
- Working with REST APIs
- Transforming JSON data using Python
- Storing structured data in PostgreSQL
- Orchestrating workflows with Apache Airflow
- Running data engineering services using Docker
- Using Git and GitHub for project version control

## Author
**Vijay Phanase**

Aspiring Data Engineer | Python | SQL | PostgreSQL | AWS | Apache Airflow
