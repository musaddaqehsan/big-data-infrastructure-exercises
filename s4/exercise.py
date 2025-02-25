import gzip
import io
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import boto3
import requests
from fastapi import APIRouter, status
from bdi_api.settings import Settings

# Configure logging with a consistent format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize settings from configuration
settings = Settings()

# Define FastAPI router with specific prefixes and response definitions
s4_router = APIRouter(
    prefix="/api/s4",
    tags=["s4"],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
)


# ---- S3 and File Handling Functions ----

def upload_to_s3(data_stream: io.BytesIO, bucket: str, key: str, s3_client: Optional[boto3.client] = None) -> None:
    """
    Uploads a file-like object to AWS S3.

    Args:
        data_stream: The file-like object to upload.
        bucket: The S3 bucket name.
        key: The S3 key (path) for the uploaded file.
        s3_client: Optional boto3 S3 client; creates a new one if not provided.
    """
    s3_client = s3_client or boto3.client("s3")
    try:
        s3_client.upload_fileobj(data_stream, bucket, key)
        logger.info(f"Successfully uploaded to S3: {bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to upload {key} to S3: {e}")
        raise


def download_gzip_stream(url: str) -> Optional[io.BytesIO]:
    """
    Downloads a GZIP file as a stream.

    Args:
        url: The URL of the GZIP file to download.

    Returns:
        A file-like stream of the downloaded content or None if the download fails.
    """
    try:
        response = requests.get(url, stream=True, timeout=24)
        response.raise_for_status()
        return response.raw
    except requests.RequestException as e:
        logger.error(f"Error downloading GZIP from {url}: {e}")
        return None


def stream_gzip_to_s3(url: str, bucket: str, key: str, s3_client: Optional[boto3.client] = None) -> None:
    """
    Downloads a GZIP file from a URL and streams it directly to S3.

    Args:
        url: The URL of the GZIP file.
        bucket: The target S3 bucket.
        key: The S3 key for the file.
        s3_client: Optional boto3 S3 client.
    """
    file_stream = download_gzip_stream(url)
    if file_stream:
        upload_to_s3(file_stream, bucket, key, s3_client)


# ---- Aircraft Data Download Endpoint ----

@s4_router.post("/aircraft/download")
def download_aircraft_data(file_limit: int = 1500, s3_client: Optional[boto3.client] = None) -> str:
    """
    Downloads aircraft data files and uploads them to an S3 bucket.

    Args:
        file_limit: Number of files to download (default: 1500).
        s3_client: Optional boto3 S3 client for dependency injection.

    Returns:
        A status message indicating the number of files downloaded and their S3 location.
    """
    base_url = f"{settings.source_url}/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix = "raw/day=20231101/"
    current_time = datetime.strptime("000000", "%H%M%S")

    for _ in range(file_limit):
        filename = current_time.strftime("%H%M%SZ.json.gz")
        file_url = f"{base_url}{filename}"
        s3_key = f"{s3_prefix}{filename}"

        stream_gzip_to_s3(file_url, s3_bucket, s3_key, s3_client)

        # Increment time by 5 seconds and handle overflow
        current_time += timedelta(seconds=5)
        if current_time.second >= 60:  # Reset seconds if they exceed 59
            current_time = current_time.replace(second=0, minute=current_time.minute + 1)

    return f"Downloaded {file_limit} files and uploaded to S3 bucket: {s3_bucket}/{s3_prefix}"


# ---- Directory and Data Processing Functions ----

def clean_directory(path: str) -> None:
    """
    Removes all files from the specified directory.

    Args:
        path: The directory path to clean.
    """
    if not os.path.exists(path):
        return

    for filename in os.listdir(path):
        file_path = os.path.join(path, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
                logger.debug(f"Deleted file: {file_path}")
        except Exception as e:
            logger.error(f"Error deleting {file_path}: {e}")


def process_aircraft_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Processes raw aircraft JSON data into a structured format.

    Args:
        data: The raw JSON data containing aircraft information.

    Returns:
        A list of processed aircraft records.
    """
    timestamp = data.get("now")
    aircraft_records = data.get("aircraft", [])

    return [
        {
            "icao": record.get("hex"),
            "registration": record.get("r"),
            "type": record.get("t"),
            "lat": record.get("lat"),
            "lon": record.get("lon"),
            "alt_baro": record.get("alt_baro"),
            "timestamp": timestamp,
            "max_altitude_baro": record.get("alt_baro"),
            "max_ground_speed": record.get("gs"),
            "had_emergency": record.get("alert", 0) == 1,
        }
        for record in aircraft_records
    ]


# ---- Aircraft Data Preparation Endpoint ----

@s4_router.post("/aircraft/prepare")
def prepare_aircraft_data(s3_client: Optional[boto3.client] = None) -> str:
    """
    Retrieves aircraft data from S3, processes it, and saves it locally.

    Args:
        s3_client: Optional boto3 S3 client for dependency injection.

    Returns:
        A status message indicating where the processed data was saved or an error message.
    """
    s3_client = s3_client or boto3.client("s3")
    s3_bucket = settings.s3_bucket
    s3_prefix = "raw/day=20231101/"
    local_dir = settings.prepared_dir

    # Prepare the local directory
    clean_directory(local_dir)
    os.makedirs(local_dir, exist_ok=True)

    try:
        # List objects in S3
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        if "Contents" not in response:
            logger.info(f"No files found in S3 bucket: {s3_bucket}/{s3_prefix}")
            return "No files found in S3."

        # Process each file
        for obj in response["Contents"]:
            s3_key = obj["Key"]
            filename = os.path.basename(s3_key)
            local_file_path = os.path.join(local_dir, filename.replace(".gz", ""))

            try:
                # Download and decompress the file
                file_obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
                compressed_stream = io.BytesIO(file_obj["Body"].read())

                with gzip.GzipFile(fileobj=compressed_stream) as gz_file:
                    raw_data = json.loads(gz_file.read().decode("utf-8"))

                # Process and save the data
                processed_data = process_aircraft_data(raw_data)
                with open(local_file_path, "w", encoding="utf-8") as output_file:
                    json.dump(processed_data, output_file)

                logger.info(f"Processed and saved: {local_file_path}")

            except Exception as e:
                logger.error(f"Error processing file {s3_key}: {e}")

        return f"Prepared data saved to {local_dir}"

    except Exception as e:
        logger.error(f"Error accessing S3: {e}")
        return "Error accessing S3."