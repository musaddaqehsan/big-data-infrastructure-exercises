import logging
from fastapi import BackgroundTasks
import io
import gzip
import json
import boto3
from fastapi import APIRouter
from bdi_api.settings import DBCredentials, Settings
import psycopg2
from psycopg2.extras import execute_batch

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

settings = Settings()
db_credentials = DBCredentials()
s3_client = boto3.client("s3")
BUCKET_NAME = "bdi-aircraft-mussi"
s7 = APIRouter(prefix="/api/s7", tags=["s7"])

def connect_to_database():
    try:
        logger.info(f"Connecting to database: {db_credentials.host}:{db_credentials.port}")
        conn = psycopg2.connect(
            dbname=db_credentials.database,
            user=db_credentials.username,
            password=db_credentials.password,
            host=db_credentials.host,
            port=db_credentials.port
        )
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def create_database_tables():
    logger.info("Creating database tables")
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aircraft (
            icao VARCHAR PRIMARY KEY,
            registration VARCHAR,
            type VARCHAR
        );
        CREATE TABLE IF NOT EXISTS aircraft_positions (
            icao VARCHAR REFERENCES aircraft(icao),
            timestamp BIGINT,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            altitude_baro DOUBLE PRECISION,
            ground_speed DOUBLE PRECISION,
            emergency BOOLEAN,
            PRIMARY KEY (icao, timestamp)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database tables created successfully")

def get_all_files_from_s3():
    logger.info(f"Fetching files from S3 bucket: {BUCKET_NAME}")
    all_data = []
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        objects = response.get("Contents", [])
        logger.info(f"Found {len(objects)} objects in S3 bucket")
        for obj in objects:
            file_key = obj["Key"]
            logger.info(f"Processing file: {file_key}")
            file_data = get_file_from_s3(file_key)
            all_data.extend(file_data)
        logger.info(f"Retrieved {len(all_data)} records from S3")
        return all_data
    except Exception as e:
        logger.error(f"Error fetching from S3: {str(e)}")
        raise

def get_file_from_s3(file_key):
    logger.info(f"Downloading S3 file: {file_key}")
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    content = obj["Body"].read()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            data = json.loads(gz.read().decode("utf-8"))
    except:
        data = json.loads(content.decode("utf-8"))
    logger.info(f"Processed S3 file: {file_key}")
    return data.get("aircraft", data) if isinstance(data, dict) else data

def save_to_database(data):
    logger.info("Saving data to database")
    conn = connect_to_database()
    cur = conn.cursor()
    
    aircraft_data = []
    position_data = []
    
    for record in data:
        if not isinstance(record, dict):
            continue
            
        icao = record.get("icao") or record.get("hex")
        if not icao:
            continue
            
        aircraft_data.append((
            icao,
            record.get("registration", ""),
            record.get("type", "")
        ))
        
        if "lat" in record and "lon" in record:
            position_data.append((
                icao,
                record.get("timestamp", 0),
                record["lat"],
                record["lon"],
                float(record.get("altitude_baro", 0)),
                float(record.get("ground_speed", 0)),
                bool(record.get("emergency", False))
            ))
    
    BATCH_SIZE = 10000  # Smaller batch size for faster commits
    if aircraft_data:
        logger.info(f"Inserting {len(aircraft_data)} aircraft records in batches of {BATCH_SIZE}")
        for i in range(0, len(aircraft_data), BATCH_SIZE):
            batch = aircraft_data[i:i + BATCH_SIZE]
            logger.info(f"Inserting aircraft batch {i // BATCH_SIZE + 1} with {len(batch)} records")
            execute_batch(cur, """
                INSERT INTO aircraft (icao, registration, type)
                VALUES (%s, %s, %s)
                ON CONFLICT (icao) DO UPDATE SET
                    registration = EXCLUDED.registration,
                    type = EXCLUDED.type
            """, batch)
            conn.commit()  
    
    if position_data:
        logger.info(f"Inserting {len(position_data)} position records in batches of {BATCH_SIZE}")
        for i in range(0, len(position_data), BATCH_SIZE):
            batch = position_data[i:i + BATCH_SIZE]
            logger.info(f"Inserting position batch {i // BATCH_SIZE + 1} with {len(batch)} records")
            execute_batch(cur, """
                INSERT INTO aircraft_positions
                (icao, timestamp, lat, lon, altitude_baro, ground_speed, emergency)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (icao, timestamp) DO NOTHING
            """, batch)
            conn.commit()  
    
    cur.close()
    conn.close()
    logger.info("Data saved to database successfully")
from fastapi import BackgroundTasks

@s7.post("/aircraft/prepare")
def prepare_data(background_tasks: BackgroundTasks):
    def process_data():
        try:
            create_database_tables()
            data = get_all_files_from_s3()
            if data:
                save_to_database(data)
                logger.info("Aircraft data saved")
            else:
                logger.info("No aircraft data found")
        except Exception as e:
            logger.error(f"Background task failed: {str(e)}")

    logger.info("Starting /aircraft/prepare endpoint")
    background_tasks.add_task(process_data)
    return {"message": "Processing started, check logs for completion"}

@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0):
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(
        "SELECT icao, registration, type FROM aircraft ORDER BY icao LIMIT %s OFFSET %s",
        (num_results, page * num_results)
    )
    results = [{"icao": r[0], "registration": r[1], "type": r[2]} for r in cur.fetchall()]
    cur.close()
    conn.close()
    return results

@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0):
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(
        "SELECT timestamp, lat, lon FROM aircraft_positions WHERE icao = %s ORDER BY timestamp LIMIT %s OFFSET %s",
        (icao, num_results, page * num_results)
    )
    results = [{"timestamp": r[0], "lat": r[1], "lon": r[2]} for r in cur.fetchall()]
    cur.close()
    conn.close()
    return results

@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str):
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(MAX(altitude_baro), 0),
               COALESCE(MAX(ground_speed), 0),
               COALESCE(BOOL_OR(emergency), FALSE)
        FROM aircraft_positions WHERE icao = %s
        """,
        (icao,)
    )
    row = cur.fetchone()
    result = {
        "max_altitude_baro": row[0],
        "max_ground_speed": row[1],
        "had_emergency": row[2]
    }
    cur.close()
    conn.close()
    return result

if __name__ == "__main__":
    print(prepare_data())