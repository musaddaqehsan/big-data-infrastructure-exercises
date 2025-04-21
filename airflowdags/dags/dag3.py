import gzip
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2.extras
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# Configuration
DATA_URL = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
RAW_BASE = "/tmp/aircraft/raw"
PREP_BASE = "/tmp/aircraft/prepared"
AWS_CONN_ID = "aws_default"
PG_CONN_ID = "postgres_default"
BUCKET = Variable.get('aircraft_s3_bucket', default_var='bdi-aircraft-mussi')
BATCH_SIZE = 10000

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def safe_int(val):
    """Convert a value to an integer, returning None if invalid."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return None

def create_directories():
    """Create local directories for raw and prepared data."""
    Path(RAW_BASE).mkdir(parents=True, exist_ok=True)
    Path(PREP_BASE).mkdir(parents=True, exist_ok=True)
    logger.info("Created/verified local directories: %s, %s", RAW_BASE, PREP_BASE)

def download_raw_data(ds, s3_hook):
    """Download raw data from URL and upload to S3 if not already present."""
    date_str = ds.replace("-", "")
    raw_dir = Path(RAW_BASE) / f"date={date_str}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_key = f"raw/basic_ac_db/date={date_str}/basic-ac-db_{date_str}.json.gz"

    if s3_hook.check_for_key(raw_key, BUCKET):
        logger.info("Raw file exists in S3: %s", raw_key)
        return {'raw_key': raw_key, 'downloaded': False}

    local_gz = raw_dir / f"basic-ac-db_{date_str}.json.gz"
    logger.info("Downloading data from %s", DATA_URL)
    try:
        response = requests.get(DATA_URL, timeout=300)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to download data from %s: %s", DATA_URL, e)
        raise

    with open(local_gz, 'wb') as f:
        f.write(response.content)

    s3_hook.load_file(
        filename=str(local_gz),
        key=raw_key,
        bucket_name=BUCKET,
        replace=True
    )
    logger.info("Uploaded raw file to S3: %s", raw_key)
    return {'raw_key': raw_key, 'downloaded': True}

def process_raw_data(raw_bytes, date_str):
    """Decompress and parse raw data into records, appending date_str."""
    try:
        data_bytes = gzip.decompress(raw_bytes)
        lines = data_bytes.decode('utf-8').splitlines()
    except gzip.BadGzipFile as e:
        logger.error("Failed to decompress raw data: %s", e)
        raise

    records = []
    for line in lines:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            logger.warning("Skipping invalid JSON line: %s", e)
            continue
        records.append([
            obj.get('icao'),
            obj.get('reg'),
            obj.get('manufacturer'),
            obj.get('model'),
            safe_int(obj.get('year')),
            obj.get('mil', False),
            obj.get('faa_pia', False),
            obj.get('faa_ladd', False),
            date_str
        ])
    logger.info("Processed %d records from raw data", len(records))
    return records

def save_prepared_data(records, prep_file):
    """Save prepared data to a JSON file."""
    with open(prep_file, 'w', encoding='utf-8') as pf:
        for record in records:
            obj = {
                'icao': record[0],
                'reg': record[1],
                'manufacturer': record[2],
                'model': record[3],
                'year': record[4],
                'mil': record[5],
                'faa_pia': record[6],
                'faa_ladd': record[7],
                'date': record[8]
            }
            pf.write(json.dumps(obj) + '\n')
    logger.info("Saved prepared data to %s", prep_file)

def upload_prepared_data(s3_hook, prep_file, prep_key):
    """Upload prepared data to S3."""
    s3_hook.load_file(
        filename=str(prep_file),
        key=prep_key,
        bucket_name=BUCKET,
        replace=True
    )
    logger.info("Uploaded prepared file to S3: %s", prep_key)

def create_aircraft_table(pg_hook):
    """Create aircraft_db table if it doesn't exist."""
    with pg_hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft_db (
                icao TEXT,
                reg TEXT,
                manufacturer TEXT,
                model TEXT,
                year INT,
                mil BOOLEAN,
                faa_pia BOOLEAN,
                faa_ladd BOOLEAN,
                date VARCHAR(8),
                PRIMARY KEY (icao, date)
            );
        """)
        conn.commit()
    logger.info("Verified aircraft_db table exists")

def load_to_postgres(pg_hook, records):
    """Load records into PostgreSQL in batches."""
    insert_sql = """
        INSERT INTO aircraft_db (
            icao, reg, manufacturer, model, year, mil, faa_pia, faa_ladd, date
        ) VALUES %s
        ON CONFLICT (icao, date) DO NOTHING;
    """
    total = len(records)
    logger.info("Inserting %d records into Postgres in batches of %d", total, BATCH_SIZE)

    with pg_hook.get_conn() as conn, conn.cursor() as cur:
        for idx in range(0, total, BATCH_SIZE):
            batch = records[idx:idx + BATCH_SIZE]
            try:
                psycopg2.extras.execute_values(cur, insert_sql, batch)
                conn.commit()
                logger.info("Inserted batch %d (records %d-%d)", idx // BATCH_SIZE + 1, idx + 1, min(idx + BATCH_SIZE, total))
            except psycopg2.Error as e:
                conn.rollback()
                logger.error("Failed to insert batch %d: %s", idx // BATCH_SIZE + 1, e)
                raise
    logger.info("Inserted %d records into Postgres", total)
    return total

def download_raw(ds, **kwargs):
    """Download raw data and upload to S3."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    return download_raw_data(ds, s3_hook)

def prepare_and_load(ds, **kwargs):
    """Prepare raw data, upload to S3, and load into PostgreSQL."""
    date_str = ds.replace("-", "")
    raw_key = f"raw/basic_ac_db/date={date_str}/basic-ac-db_{date_str}.json.gz"
    prep_dir = Path(PREP_BASE) / f"date={date_str}"
    prep_dir.mkdir(parents=True, exist_ok=True)
    prep_file = prep_dir / f"basic-ac-db_prepared_{date_str}.json"
    prep_key = f"prepared/basic_ac_db/date={date_str}/basic-ac-db_prepared_{date_str}.json"

    try:
        # Retrieve raw data from S3
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        client = s3_hook.get_conn()
        obj = client.get_object(Bucket=BUCKET, Key=raw_key)
        raw_bytes = obj['Body'].read()

        # Process and save data
        records = process_raw_data(raw_bytes, date_str)
        save_prepared_data(records, prep_file)
        upload_prepared_data(s3_hook, prep_file, prep_key)

        # Load to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        create_aircraft_table(pg_hook)
        inserted_count = load_to_postgres(pg_hook, records)

        return {'inserted': inserted_count}

    except (requests.RequestException, json.JSONDecodeError, psycopg2.Error, Exception) as e:
        logger.error("Error in prepare_and_load for date %s: %s", date_str, e)
        raise

# DAG definition
with DAG(
    'aircraft_db',
    default_args=default_args,
    description='ETL pipeline for ADS-B aircraft database to S3 and Postgres',
    schedule_interval='0 0 * * 0',  # Weekly on Sunday at midnight
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=['aircraft', 'data-inserting'],
    doc_md="""
    This DAG downloads stores the raw data in S3,
    prepares the data, uploads the prepared data to S3, and loads it into PostgreSQL.
    """
) as dag:

    create_dirs_task = PythonOperator(
        task_id='initialize_directories',
        python_callable=create_directories,
        doc_md="Creates temporary directories for raw and prepared data."
    )

    download_task = PythonOperator(
        task_id='download_raw_data',
        python_callable=download_raw,
        provide_context=True,
        doc_md="Downloads raw aircraft data and uploads it to S3."
    )

    prepare_task = PythonOperator(
        task_id='prepare_and_load_data',
        python_callable=prepare_and_load,
        provide_context=True,
        doc_md="Processes raw data, uploads prepared data to S3, and loads it into PostgreSQL."
    )

    create_dirs_task >> download_task >> prepare_task