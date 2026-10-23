
from datetime import datetime

from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, task

LATITUDE = "19.2167"
LONGITUDE = "73.0833"

POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "open_meteo_api"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}


with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["weather", "ETL"],
) as dag:

    # 1. EXTRACT
    @task()
    def extract_weather_data():
        """Fetch weather data from Open-Meteo."""

        http_hook = HttpHook(
            http_conn_id=API_CONN_ID,
            method="GET",
        )

        endpoint = (
            f"/v1/forecast?latitude={LATITUDE}"
            f"&longitude={LONGITUDE}"
            "&current=temperature_2m,relative_humidity_2m,"
            "apparent_temperature,is_day,precipitation,"
            "weather_code,wind_speed_10m"
            "&timezone=Asia%2FKolkata"
        )

        response = http_hook.run(endpoint)
        response.raise_for_status()

        return response.json()

    # 2. TRANSFORM
    @task()
    def transform_weather_data(weather_data):
        """Select and clean the required weather fields."""

        current_weather = weather_data["current"]

        return {
            "latitude": weather_data["latitude"],
            "longitude": weather_data["longitude"],
            "time": current_weather["time"],
            "temperature": current_weather["temperature_2m"],
            "humidity": current_weather["relative_humidity_2m"],
            "apparent_temperature": current_weather[
                "apparent_temperature"
            ],
            "precipitation": current_weather["precipitation"],
            "wind_speed": current_weather["wind_speed_10m"],
            "weather_code": current_weather["weather_code"],
            "is_day": current_weather["is_day"],
        }

    # 3. LOAD
    @task()
    def load_weather_data(transformed_data):
        """Create the table and insert weather data."""

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        create_table_sql = """
            CREATE TABLE IF NOT EXISTS weather_data (
                id BIGSERIAL PRIMARY KEY,
                latitude NUMERIC(10, 6) NOT NULL,
                longitude NUMERIC(10, 6) NOT NULL,
                time TIMESTAMP NOT NULL,
                temperature NUMERIC(5, 2),
                humidity NUMERIC(5, 2),
                apparent_temperature NUMERIC(5, 2),
                precipitation NUMERIC(8, 2),
                wind_speed NUMERIC(6, 2),
                weather_code INTEGER,
                is_day SMALLINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """

        pg_hook.run(create_table_sql)

        pg_hook.run("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            unique_weather_reading
            ON weather_data (latitude, longitude, time);
        """)

        insert_sql = """
            INSERT INTO weather_data (
                latitude, longitude, time, temperature,
                humidity, apparent_temperature, precipitation,
                wind_speed, weather_code, is_day
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (latitude, longitude, time)
            DO NOTHING;
        """

        values = (
            transformed_data["latitude"],
            transformed_data["longitude"],
            transformed_data["time"],
            transformed_data["temperature"],
            transformed_data["humidity"],
            transformed_data["apparent_temperature"],
            transformed_data["precipitation"],
            transformed_data["wind_speed"],
            transformed_data["weather_code"],
            transformed_data["is_day"],
        )

        pg_hook.run(insert_sql, parameters=values)

        print("Weather data loaded successfully!")

    # Connect the ETL tasks
    raw_data = extract_weather_data()
    clean_data = transform_weather_data(raw_data)
    load_weather_data(clean_data)
