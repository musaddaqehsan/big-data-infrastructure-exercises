from datetime import datetime, timedelta
import json
import os
import requests
import logging
import tempfile
import boto3
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

# Configuration
FUEL_RATES_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
DATA_DIR = os.path.join(tempfile.gettempdir(), "airflow_fuel_rates")
S3_BUCKET = Variable.get('aircraft_s3_bucket', default_var='bdi-aircraft-mussi')
POSTGRES_CONN_ID = 'postgres_default'

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

def ensure_data_directory():
    """Ensure the data directory exists."""
    os.makedirs(DATA_DIR, exist_ok=True)
    logger.info(f"Created/verified data directory: {DATA_DIR}")
    return True

def download_json_data(url, file_path, latest_path):
    """Download JSON data from a URL and save to files."""
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            logger.info(f"Valid JSON found at {file_path} with {len(data)} records. Skipping download.")
            return data, False
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON at {file_path}. Re-downloading.")

    logger.info(f"Downloading data from {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)
    with open(latest_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    logger.info(f"Saved JSON data to {file_path} and {latest_path}")
    return data, True

def upload_to_s3(file_path, s3_key, latest_path, latest_key):
    """Upload files to S3."""
    s3_client = boto3.client('s3')
    s3_client.upload_file(file_path, S3_BUCKET, s3_key)
    s3_client.upload_file(latest_path, S3_BUCKET, latest_key)
    logger.info(f"Uploaded to S3: s3://{S3_BUCKET}/{s3_key} and s3://{S3_BUCKET}/{latest_key}")

def download_fuel_rates(**context):
    """Download aircraft fuel consumption rates and upload to S3."""
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y%m%d')
    file_path = os.path.join(DATA_DIR, f"fuel_rates_{date_str}.json")
    latest_path = os.path.join(DATA_DIR, "fuel_rates_latest.json")
    s3_dated_key = f"fuel_rates/fuel_rates_{date_str}.json"
    s3_latest_key = "fuel_rates/fuel_rates_latest.json"

    try:
        fuel_data, downloaded = download_json_data(FUEL_RATES_URL, file_path, latest_path)
        upload_to_s3(file_path, s3_dated_key, latest_path, s3_latest_key)
        return {
            "file_path": file_path,
            "downloaded": downloaded,
            "record_count": len(fuel_data),
            "s3_key": s3_dated_key
        }
    except (requests.RequestException, json.JSONDecodeError) as e:
        logger.error(f"Failed to download or process fuel rates: {e}")
        raise

def get_postgres_connection():
    """Retrieve PostgreSQL connection from Airflow."""
    conn_details = BaseHook.get_connection(POSTGRES_CONN_ID)
    return psycopg2.connect(
        host=conn_details.host,
        port=conn_details.port or 5432,
        database=conn_details.schema,
        user=conn_details.login,
        password=conn_details.password
    )

def create_tables(cursor):
    """Create necessary PostgreSQL tables if they don't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aircraft_fuel_rates (
            icao_type VARCHAR(10) PRIMARY KEY,
            manufacturer VARCHAR(100),
            type VARCHAR(100),
            category VARCHAR(50),
            galph FLOAT,
            kgph FLOAT,
            updated_at TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fuel_rates_reports (
            report_date DATE PRIMARY KEY,
            total_aircraft_types INTEGER,
            aircraft_with_fuel_data INTEGER,
            coverage_percentage FLOAT,
            report_data JSONB,
            created_at TIMESTAMP
        )
    """)

def process_fuel_data(fuel_data, cursor):
    """Process fuel data and insert into PostgreSQL."""
    total_aircraft = len(fuel_data)
    aircraft_with_galph = sum(1 for data in fuel_data.values() if data.get('galph') is not None)
    categories = {}

    for icaotype, data in fuel_data.items():
        category = data.get('category')
        galph = data.get('galph')
        
        if category and galph is not None:
            categories.setdefault(category, {"count": 0, "total_galph": 0})
            categories[category]["count"] += 1
            categories[category]["total_galph"] += galph
        
        cursor.execute("""
            INSERT INTO aircraft_fuel_rates 
            (icao_type, manufacturer, type, category, galph, kgph, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (icao_type) 
            DO UPDATE SET 
                manufacturer = EXCLUDED.manufacturer,
                type = EXCLUDED.type,
                category = EXCLUDED.category,
                galph = EXCLUDED.galph,
                kgph = EXCLUDED.kgph,
                updated_at = EXCLUDED.updated_at
        """, (
            icaotype,
            data.get('manufacturer'),
            data.get('type'),
            data.get('category'),
            data.get('galph'),
            data.get('galph', 0) * 3.79 if data.get('galph') is not None else None,
            datetime.now()
        ))
    
    for category in categories:
        if categories[category]["count"] > 0:
            categories[category]["avg_galph"] = categories[category]["total_galph"] / categories[category]["count"]
    
    return total_aircraft, aircraft_with_galph, categories

def generate_report(total_aircraft, aircraft_with_galph, categories):
    """Generate analysis report."""
    return {
        "timestamp": datetime.now().isoformat(),
        "total_aircraft_types": total_aircraft,
        "aircraft_with_fuel_data": aircraft_with_galph,
        "coverage_percentage": round(aircraft_with_galph / total_aircraft * 100, 2) if total_aircraft > 0 else 0,
        "categories": {
            category: {
                "count": data["count"],
                "avg_galph": round(data.get("avg_galph", 0), 2),
                "avg_kg_per_hour": round(data.get("avg_galph", 0) * 3.79, 2)
            } for category, data in categories.items()
        }
    }

def store_report(report, report_path, s3_report_key, cursor, execution_date):
    """Save report to file, S3, and PostgreSQL."""
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    s3_client = boto3.client('s3')
    s3_client.upload_file(report_path, S3_BUCKET, s3_report_key)
    logger.info(f"Uploaded report to S3: s3://{S3_BUCKET}/{s3_report_key}")
    
    cursor.execute("""
        INSERT INTO fuel_rates_reports
        (report_date, total_aircraft_types, aircraft_with_fuel_data, coverage_percentage, report_data, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (report_date) 
        DO UPDATE SET 
            total_aircraft_types = EXCLUDED.total_aircraft_types,
            aircraft_with_fuel_data = EXCLUDED.aircraft_with_fuel_data,
            coverage_percentage = EXCLUDED.coverage_percentage,
            report_data = EXCLUDED.report_data,
            created_at = EXCLUDED.created_at
    """, (
        execution_date.date(),
        report["total_aircraft_types"],
        report["aircraft_with_fuel_data"],
        report["coverage_percentage"],
        json.dumps(report),
        datetime.now()
    ))

def analyze_fuel_rates(**context):
    """Analyze fuel consumption rates, generate report, and store in PostgreSQL."""
    ti = context['ti']
    download_result = ti.xcom_pull(task_ids='download_fuel_rates')
    
    if not download_result or 'file_path' not in download_result:
        logger.error("Missing download information from previous task")
        raise ValueError("Missing download information")
    
    file_path = download_result['file_path']
    execution_date = context['execution_date']
    report_path = os.path.join(DATA_DIR, f"fuel_rates_report_{execution_date.strftime('%Y%m%d')}.json")
    s3_report_key = f"fuel_rates/reports/fuel_rates_report_{execution_date.strftime('%Y%m%d')}.json"
    
    try:
        with open(file_path, 'r') as f:
            fuel_data = json.load(f)
        
        with get_postgres_connection() as conn, conn.cursor() as cursor:
            create_tables(cursor)
            total_aircraft, aircraft_with_galph, categories = process_fuel_data(fuel_data, cursor)
            report = generate_report(total_aircraft, aircraft_with_galph, categories)
            store_report(report, report_path, s3_report_key, cursor, execution_date)
            conn.commit()
        
        logger.info(f"Processed {total_aircraft} aircraft types and stored report")
        return {
            "report_path": report_path,
            "total_aircraft_types": total_aircraft,
            "coverage_percentage": report["coverage_percentage"],
            "data_dir": DATA_DIR,
            "s3_report_key": s3_report_key
        }
    
    except Exception as e:
        logger.error(f"Error analyzing fuel rates: {e}")
        raise

# Define the DAG
with DAG(
    'fuel_consumption_of_aircraft',
    default_args=default_args,
    description='Download and analyze aircraft fuel consumption rates',
    schedule_interval='0 0 * * 1',  # Weekly on Monday at midnight
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=['aircraft', 'fuel-consumption'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    ensure_directories = PythonOperator(
        task_id='ensure_directories',
        python_callable=ensure_data_directory
    )
    download_task = PythonOperator(
        task_id='download_fuel_rates',
        python_callable=download_fuel_rates
    )
    analyze_task = PythonOperator(
        task_id='analyze_fuel_rates',
        python_callable=analyze_fuel_rates
    )
    end = EmptyOperator(task_id='end')
    
    start >> ensure_directories >> download_task >> analyze_task >> end