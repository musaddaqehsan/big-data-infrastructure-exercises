import gzip
import json
import logging
from datetime import timedelta
from pathlib import Path
from urllib.parse import urljoin

import pendulum
import psycopg2.extras
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DATE_MIN = pendulum.datetime(2023, 11, 1, tz="UTC")
DATE_MAX = pendulum.datetime(2024, 11, 1, tz="UTC")
RAW_BASE = "/tmp/readsb/raw"
PREP_BASE = "/tmp/readsb/prepared"
BASE_URL = "https://samples.adsbexchange.com/readsb-hist"
AWS_CONN_ID = "aws_default"
PG_CONN_ID = "postgres_default"
BUCKET = Variable.get("aircraft_s3_bucket", default_var="bdi-aircraft-mussi")
FILE_LIMIT = int(Variable.get("readsb_file_limit", default_var=100))
BATCH_SIZE = 1000

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

# Helper functions
def get_first_of_month(dt):
    """Return the first day of the month for a given datetime."""
    return dt.replace(day=1)

def valid_date(dt):
    """Check if the date is within the valid range and is the first of the month."""
    first = get_first_of_month(dt)
    return first.day == 1 and DATE_MIN <= first <= DATE_MAX

def safe_int_alt(alt):
    """Convert altitude to integer, handling 'ground' and invalid values."""
    if isinstance(alt, (int, float)):
        return int(alt)
    if alt == "ground":
        return 0
    return None

# Task functions
def create_directories():
    """Create local directories for raw and prepared data."""
    Path(RAW_BASE).mkdir(parents=True, exist_ok=True)
    Path(PREP_BASE).mkdir(parents=True, exist_ok=True)
    logger.info("Created/verified local directories: %s, %s", RAW_BASE, PREP_BASE)

def download_file(url, local_file, s3_hook, s3_key):
    """Download a file and upload it to S3."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        with open(local_file, "wb") as f:
            f.write(response.content)
        s3_hook.load_file(
            filename=str(local_file),
            key=s3_key,
            bucket_name=BUCKET,
            replace=True
        )
        logger.info("Downloaded and uploaded: %s to S3: %s", local_file.name, s3_key)
        return True
    except requests.RequestException as e:
        logger.warning("Failed to download %s: %s", url, e)
        return False

def download_data(ds, **kwargs):
    """Download raw data files for the first of the month and upload to S3."""
    exec_dt = pendulum.parse(ds)
    if not valid_date(exec_dt):
        raise AirflowSkipException(f"Execution date {ds} not in valid range")

    date_str = exec_dt.format("YYYYMM") + "01"
    raw_dir = Path(RAW_BASE) / f"day={date_str}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    downloaded = 0
    current = pendulum.parse("00:00:00", strict=False)

    year = exec_dt.format("YYYY")
    month = exec_dt.format("MM")
    day = "01"

    for _ in range(FILE_LIMIT + 5):
        if downloaded >= FILE_LIMIT:
            break

        fname = current.format("HHmmss") + "Z.json.gz"
        s3_key = f"raw/readsb/day={date_str}/{fname}"

        if s3_hook.check_for_key(key=s3_key, bucket_name=BUCKET):
            logger.info("Skipping existing file in S3: %s", s3_key)
        else:
            url = urljoin(f"{BASE_URL}/{year}/{month}/{day}/", fname)
            local_file = raw_dir / fname
            if download_file(url, local_file, s3_hook, s3_key):
                downloaded += 1

        current = current.add(seconds=5)

    logger.info("Downloaded %d raw files for date %s", downloaded, date_str)
    return {"downloaded": downloaded, "date": date_str}

def create_aircraft_table(pg_hook):
    """Create aircraft_data table if it doesn't exist and ensure file_date column."""
    with pg_hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft_data (
                id SERIAL PRIMARY KEY,
                icao VARCHAR(10),
                registration VARCHAR(20),
                type VARCHAR(20),
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                alt_baro INTEGER,
                timestamp BIGINT,
                max_altitude_baro INTEGER,
                max_ground_speed DOUBLE PRECISION,
                had_emergency BOOLEAN,
                file_date VARCHAR(8),
                file_name VARCHAR(50),
                UNIQUE(icao, timestamp, file_name)
            );
        """)
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'aircraft_data' AND column_name = 'file_date';
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE aircraft_data ADD COLUMN file_date VARCHAR(8);")
            logger.info("Added file_date column to aircraft_data table")
        conn.commit()
    logger.info("Verified aircraft_data table structure")

def process_file(file_path, date_str):
    """Process a raw data file and extract records."""
    records = []
    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, gzip.BadGzipFile, json.JSONDecodeError):
        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.error("Failed to parse %s: %s", file_path.name, e)
            return records

    ts = data.get("now")
    for ac in data.get("aircraft", []):
        alt = safe_int_alt(ac.get("alt_baro"))
        if alt is None or ac.get("hex") is None:
            continue
        records.append((
            ac.get("hex"),
            ac.get("r", ""),
            ac.get("t", ""),
            ac.get("lat"),
            ac.get("lon"),
            alt,
            ts,
            alt,
            ac.get("gs"),
            ac.get("alert", 0) == 1,
            date_str,
            file_path.name
        ))
    return records

def save_and_upload_prepared(records, prep_file, prep_key, s3_hook):
    """Save prepared records to a file and upload to S3."""
    with open(prep_file, "w", encoding="utf-8") as pf:
        json.dump([{
            "icao": r[0], "registration": r[1], "type": r[2], "lat": r[3], "lon": r[4],
            "alt_baro": r[5], "timestamp": r[6], "max_altitude_baro": r[7],
            "max_ground_speed": r[8], "had_emergency": r[9], "file_date": r[10],
            "file_name": r[11]
        } for r in records], pf)
    s3_hook.load_file(
        filename=str(prep_file),
        key=prep_key,
        bucket_name=BUCKET,
        replace=True
    )
    logger.info("Uploaded prepared file to S3: %s", prep_key)

def load_to_postgres(pg_hook, records):
    """Load records into PostgreSQL in batches."""
    insert_sql = """
        INSERT INTO aircraft_data (
            icao, registration, type, lat, lon,
            alt_baro, timestamp, max_altitude_baro,
            max_ground_speed, had_emergency,
            file_date, file_name
        ) VALUES %s
        ON CONFLICT (icao, timestamp, file_name) DO NOTHING;
    """
    total = len(records)
    logger.info("Inserting %d records into Postgres in batches of %d", total, BATCH_SIZE)

    with pg_hook.get_conn() as conn, conn.cursor() as cur:
        for i in range(0, total, BATCH_SIZE):
            try:
                psycopg2.extras.execute_values(cur, insert_sql, records[i:i + BATCH_SIZE])
                conn.commit()
                logger.info("Inserted batch %d with %d rows", i // BATCH_SIZE + 1, len(records[i:i + BATCH_SIZE]))
            except psycopg2.Error as e:
                conn.rollback()
                logger.error("Failed to insert batch %d: %s", i // BATCH_SIZE + 1, e)
                raise
    return total

def prepare_data(ds, **kwargs):
    """Prepare raw data, upload to S3, and load into PostgreSQL."""
    exec_dt = pendulum.parse(ds)
    if not valid_date(exec_dt):
        raise AirflowSkipException(f"Execution date {ds} not in valid range")

    date_str = exec_dt.format("YYYYMM") + "01"
    raw_dir = Path(RAW_BASE) / f"day={date_str}"
    prep_dir = Path(PREP_BASE) / f"day={date_str}"
    prep_dir.mkdir(parents=True, exist_ok=True)

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    try:
        create_aircraft_table(pg_hook)
        all_records = []

        for gz in raw_dir.glob("*.json.gz"):
            records = process_file(gz, date_str)
            if not records:
                continue

            prep_file = prep_dir / gz.name.replace(".json.gz", ".json")
            prep_key = f"prepared/readsb/day={date_str}/{prep_file.name}"
            save_and_upload_prepared(records, prep_file, prep_key, s3_hook)
            all_records.extend(records)

        if all_records:
            inserted = load_to_postgres(pg_hook, all_records)
            logger.info("Processed and inserted %d records for date %s", inserted, date_str)
        else:
            logger.info("No valid records found for date %s", date_str)

        return {"records": len(all_records), "date": date_str}

    except (requests.RequestException, json.JSONDecodeError, psycopg2.Error) as e:
        logger.error("Error processing data for date %s: %s", date_str, e)
        raise

# DAG definition
with DAG(
    dag_id="readsb_hist",
    default_args=default_args,
    description="Monthly ETL of ADS-B Exchange readsb-hist data to S3 and Postgres",
    schedule_interval="@monthly",
    start_date=DATE_MIN,
    end_date=DATE_MAX,
    catchup=True,
    max_active_runs=1,
    tags=["aviation", "adsb-data", "etl-pipeline"],
) as dag:

    init_task = PythonOperator(
        task_id="initialize_directories",
        python_callable=create_directories,
        doc_md="Create temporary directories for raw and prepared data."
    )

    download_task = PythonOperator(
        task_id="download_rawData",
        python_callable=download_data,
        provide_context=True,
        doc_md="Download raw data files and upload to S3."
    )

    prepare_task = PythonOperator(
        task_id="prepare_and_load_data_to_postgres",
        python_callable=prepare_data,
        provide_context=True,
        doc_md="Prepare raw data, upload to S3, and load into PostgreSQL."
    )

    init_task >> download_task >> prepare_task