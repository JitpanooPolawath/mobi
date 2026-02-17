from airflow.sdk import dag, task
import numpy as np
import pandas as pd
import psycopg
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import io
import pendulum
import glob

# CORE COLUMNS
CORE_COLUMNS = [
    "departure",
    "return",
    "bike",
    "electric_bike",
    "departure_station",
    "return_station",
    "membership_type",
    "covered_distance_m",
    "duration_sec",
    "departure_temperature_c",
    "return_temperature_c",
    "stopover_duration_sec",
    "number_of_stopovers",
]

# COLUMN NAME NORMALIZATION MAP
COLUMN_MAP = {
    # Departure / Return timestamps
    "departure":                        "departure",
    "return":                           "return",

    # Bike info
    "bike":                             "bike",
    "electric":                         "electric_bike",
    "electric bike":                    "electric_bike",

    # Stations
    "departure station":                "departure_station",
    "return station":                   "return_station",

    # Membership
    "membership type":                  "membership_type",
    "formula":                          "membership_type",

    # Trip metrics
    "covered distance (m)":             "covered_distance_m",
    "duration (sec.)":                  "duration_sec",
    "departure temperature (c)":       "departure_temperature_c",
    "return temperature (c)":          "return_temperature_c",
    "departure temperature (°c)":       "departure_temperature_c",
    "return temperature (°c)":          "return_temperature_c",
    "stopover duration":                "stopover_duration_sec",
    "stopover duration (sec.)":         "stopover_duration_sec",

    # Stopovers - two known variants
    "number of stopovers":              "number_of_stopovers",
    "number of bike stopovers":         "number_of_stopovers",
}


def read_file(filepath: str) -> pd.DataFrame:
    """Read a CSV or XLSX file into a dataframe."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(filepath)

    if ext == ".csv":
        encodings = ["utf-8", "latin-1", "cp1252"]
        for encoding in encodings:
            try:
                df = pd.read_csv(filepath, low_memory=False, encoding=encoding)
                print(f"  Read with encoding: {encoding}")
                return df
            except UnicodeDecodeError:
                print(f"  Encoding {encoding} failed, trying next...")
                continue

        raise ValueError(f"Could not read {filepath} with any known encoding")

    raise ValueError(f"Unsupported file type: {ext}")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Lowercase + strip all column headers
    # Rename using COLUMN_MAP
    # Drop any columns not in CORE_COLUMNS
    # Add missing optional columns with defaults
    
    df.columns = [col.strip().lower() for col in df.columns]

    df = df.rename(columns=COLUMN_MAP)

    extra_cols = [col for col in df.columns if col not in CORE_COLUMNS]
    if extra_cols:
        print(f"  Dropping extra columns: {extra_cols}")
    df = df[[col for col in df.columns if col in CORE_COLUMNS]]

    if "electric_bike" not in df.columns:
        print(f"  'electric_bike' column not found - defaulting to False")
        df["electric_bike"] = False

    for col in CORE_COLUMNS:
        if col not in df.columns:
            print(f"  '{col}' column not found - defaulting to None")
            df[col] = None

    return df[CORE_COLUMNS]


def parse_filename_date(filename: str) -> str:
    """
    Mobi_System_Data_2018-01.csv -> '2018-01'
    Mobi_System_Data_2017.xlsx   -> '2017'
    """
    base = os.path.splitext(os.path.basename(filename))[0]
    parts = base.split("_")
    return parts[-1]  # last segment is always the date portion


# SNOWFLAKE CONNECTION
def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

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
    dag_id="bike_trips_etl",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    description="Extract bike trip CSVs/XLSXs from local data/raw/ and load into Snowflake",
)
def bike_trips_etl():
    @task()
    def extract(**context):
        raw_path = os.path.join(os.path.dirname(__file__), "../data/raw/")
        
        # Collect all CSV and XLSX files
        files = (
            glob.glob(os.path.join(raw_path, "*.csv")) +
            glob.glob(os.path.join(raw_path, "*.xlsx")) +
            glob.glob(os.path.join(raw_path, "*.xls"))
        )
        if not files:
            raise FileNotFoundError(f"No CSV or XLSX files found in data/raw/")

        # Sort files by their date portion so earlier files are processed first
        files = sorted(files, key=parse_filename_date)
        print(f"Found {len(files)} files to process")

        dfs = []
        for file in files:
            filename = os.path.basename(file)
            print(f"Processing: {filename}")
            try:
                df = read_file(file)
                df = normalize_columns(df)
                df["source_file"] = filename
                print(f"  -> {len(df)} rows loaded")
                dfs.append(df)
            except Exception as e:
                print(f"  WARNING: Skipping {filename} due to error: {e}")
                continue

        dfs = [df for df in dfs if not df.empty and len(df) > 0]

        if not dfs:
            raise ValueError("No files were successfully processed")

        combined = pd.concat(dfs, ignore_index=True)
        print(f"\nTotal rows before deduplication: {len(combined)}")

        # Deduplicate
        before = len(combined)
        combined = combined.drop_duplicates(
            subset=["departure", "bike", "departure_station"],
            keep="first"
        )
        after = len(combined)
        print(f"Dropped {before - after} duplicate rows")
        print(f"Total rows after deduplication: {after}")


        # Timestamps
        combined["departure"] = pd.to_datetime(combined["departure"], errors="coerce")
        combined["return"]    = pd.to_datetime(combined["return"],    errors="coerce")

        # bike is INT
        combined["bike"] = pd.to_numeric(combined["bike"], errors="coerce").astype("Int64")

        # electric_bike is BOOLEAN
        combined["electric_bike"] = combined["electric_bike"].fillna(False).astype(bool)

        # string columns - ensure they are str not mixed types
        for col in ["departure_station", "return_station", "membership_type", "source_file"]:
            combined[col] = combined[col].astype(str).str.strip()

        # float columns
        float_cols = [
            "covered_distance_m",
            "duration_sec",
            "departure_temperature_c",
            "return_temperature_c",
            "stopover_duration_sec",
            "number_of_stopovers",
        ]
        for col in float_cols:
            combined[col] = pd.to_numeric(combined[col], errors="coerce").astype(float)


        staging_path = "/tmp/mobi_staging.parquet"
        combined.to_parquet(staging_path, index=False)
        
        # Push to XCom using return value (preferred in Airflow 3.x)
        return staging_path

    @task()
    def transform(staging_path: str):
        if not staging_path or not os.path.exists(staging_path):
            raise FileNotFoundError(f"Staging file not found: {staging_path}. Did extract task run successfully?")

        df = pd.read_parquet(staging_path)

        # These fields must never be null
        critical_fields = ["departure", "departure_station", "return_station"]

        null_counts = df[CORE_COLUMNS].isnull().sum()
        print(f"Null counts per column:\n{null_counts}")

        before = len(df)
        df = df.dropna(subset=critical_fields)
        after = len(df)
        print(f"Dropped {before - after} rows missing critical fields")

        # Ensure electric_bike is boolean
        df["electric_bike"] = df["electric_bike"].fillna(False).astype(bool)

        # Ensure numeric fields are cast correctly
        df["covered_distance_m"] = pd.to_numeric(df["covered_distance_m"], errors="coerce").astype(float)
        df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce").astype("Int64").astype(float)
        df["departure_temperature_c"] = pd.to_numeric(df["departure_temperature_c"], errors="coerce").astype(float)
        df["return_temperature_c"] = pd.to_numeric(df["return_temperature_c"], errors="coerce").astype(float)
        df["stopover_duration_sec"] = pd.to_numeric(df["stopover_duration_sec"], errors="coerce").astype(float)
        df["number_of_stopovers"] = pd.to_numeric(df["number_of_stopovers"], errors="coerce").astype(float)
        df["bike"] = pd.to_numeric(df["bike"], errors="coerce").astype("Int64")

        df.to_parquet(staging_path, index=False)
        print(f"Validation passed. {after} clean rows ready to load.")
        
        return staging_path

    # @task()  
    # def load(staging_path: str):
    #     if not staging_path or not os.path.exists(staging_path):
    #         raise FileNotFoundError(f"Staging file not found: {staging_path}. Did validate task run successfully?")

    #     df = pd.read_parquet(staging_path)

    #     conn = get_snowflake_conn()
    #     try:
    #         # write_pandas handles staging and COPY INTO automatically
    #         success, nchunks, nrows, _ = write_pandas(
    #             conn=conn,
    #             df=df,
    #             table_name='raw_bike_trips',
    #             auto_create_table=True
    #         )

    #         if success:
    #             print(f"Successfully loaded {nrows} rows into RAW_BIKE_TRIPS")
                
    #     finally:
    #         conn.commit()
    #         conn.close()
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
                CREATE TABLE IF NOT EXISTS raw_bike_trips (
                    departure TIMESTAMP,
                    return TIMESTAMP,
                    bike BIGINT,
                    electric_bike BOOLEAN,
                    departure_station TEXT,
                    return_station TEXT,
                    membership_type TEXT,
                    covered_distance_m REAL,
                    duration_sec REAL,
                    departure_temperature_c REAL,
                    return_temperature_c REAL,
                    stopover_duration_sec REAL,
                    number_of_stopovers REAL,
                    source_file TEXT
                );
                """
                cur.execute(create_table_sql)
                
                with cur.copy("COPY raw_bike_trips FROM STDIN") as copy:
                    for row in df.itertuples(index=False, name=None):
                        clean_row = tuple(None if val is pd.NA or val != val else val for val in row)
                        copy.write_row(clean_row)

                nrows = len(df)
                print(f"Successfully loaded {nrows} rows into raw_bike_trips")
                
        finally:
            conn.commit()
            conn.close()

    # Define task dependencies
    staging_path = extract()
    validated_path = transform(staging_path)
    load(validated_path)

bike_trips_etl()