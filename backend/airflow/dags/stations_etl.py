import requests
from airflow.sdk import dag, task
import numpy as np
import pandas as pd
import psycopg
import os
import io
import pendulum
import glob

# GET stations from https://www.mobibikes.ca/api/client/entities

# POSTGRES CONNECTION
def get_postgres_conn():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST","postgres-data"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DATABASE"),
    )

@dag(
        dag_id="stations_etl",
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        description="Extract stations from url and load into station-lat-long",
)
def stations_etl():
    print("In etl")

    @task()
    def extract():
        response = requests.get("https://www.mobibikes.ca/api/client/entities")
        if response.status_code == 200:
            print("Successfull - 200")
        elif response.status_code == 304:
            print("Successfill redirect - 304")
        else:
            raise Exception(f"Unsuccessfull - {response.status_code} ")
        
        # Getting the content
        data = response.json()
        stations = data['data']['stations']

        data_stations = []
        for station in stations:
            label = station['label']
            coordinates = station['coordinates']
            longitude = coordinates[0]
            latitude = coordinates[1]
            row = [label,longitude,latitude]
            data_stations.append(row)
        df = pd.DataFrame(data_stations, columns=['station', 'longitude', 'latitude'])
        print(df)

        staging_path = "/tmp/mobi_station_staging.parquet"
        df.to_parquet(staging_path, index=False)

        return staging_path

    @task()  
    def load(staging_path: str):
        if not staging_path or not os.path.exists(staging_path):
            raise FileNotFoundError(f"Staging file not found: {staging_path}. Did validate task run successfully?")

        df = pd.read_parquet(staging_path)

        conn = get_postgres_conn()
        try:
            with conn.cursor() as cur:
                # Create table if it doesn't exist
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS stations (
                    station TEXT,
                    longitude REAL,
                    latitude REAL
                );
                """
                cur.execute(create_table_sql)
                
                with cur.copy("COPY stations FROM STDIN") as copy:
                    for row in df.itertuples(index=False, name=None):
                        clean_row = tuple(None if val is pd.NA or val != val else val for val in row)
                        copy.write_row(clean_row)

                nrows = len(df)
                print(f"Successfully loaded {nrows} rows into stations")
                
        finally:
            conn.commit()
            conn.close()

    extract_path = extract()
    load(extract_path)

stations_etl()