import requests
import json

from fastapi.params import Query
import os

from datetime import datetime, timedelta
from fastapi import APIRouter, status

from bdi_api.settings import Settings
import shutil

from typing import Annotated
import gzip
from fastapi.params import Query

# Initializing the API router
s1 = APIRouter(
    prefix="/api/s1",
    tags=["s1"],
    responses={  # Define any custom error responses
        404: {"description": "Not found"},
        422: {"description": "Invalid request"},
    },
)

# implementing the settings and paths for raw and prepared data
settings = Settings()
raw_data_directory = os.path.join(settings.raw_dir, "day=20231101")
prepared_data_directory = os.path.join(settings.prepared_dir, "day=20231101")


#implemented some utility functions with Teaching assistant(miquel)

def clear_folder(directory: str): #utility function
    if os.path.exists(directory):
        shutil.rmtree(directory)  # Deletes the folder and all its contents that were downloaded in the previousss attempt
    os.makedirs(directory)  # Recreates an empty folder


def fetch_gzip_file(url: str, destination_path: str): #utility functiom """Downloads a GZIP file and saves it locally."""
    try:
        response = requests.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            with open(destination_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded: {url} -> {destination_path}")
        else:
            print(f"Failed to fetch {url} (Status Code: {response.status_code})")
    except requests.exceptions.RequestException as e:
        print(f"Error while downloading {url}: {e}")


def process_aircraft_data(aircraft_records, timestamp: str) -> list: #utility function
    """Processes the aircraft data by extracting and formatting necessary fields."""
    processed_records = []
    for record in aircraft_records:
        processed_record = {
            "registration": record.get("r", None),  # Aircraft registration number
            "icao": record.get("hex", None),  # Aircraft unique ICAO code
            "type": record.get("t", None),  # Aircraft type


            "lat": record.get("lat", None),  # Latitude
            "lon": record.get("lon", None),  # Longitude
            "alt_baro": record.get("alt_baro", None),  # Barometric altitude
            "timestamp": timestamp,  # Timestamp of the data


            "max_altitude_baro": record.get("alt_baro", None),  # Maximum barometric altitude
            "max_ground_speed": record.get("gs", None),  # Maximum ground speed
            "had_emergency": record.get("alert", 0) == 1,  # Emergency alert flag
        }
        processed_records.append(processed_record)
    return processed_records


@s1.post("/aircraft/download")
def download_aircraft_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 1500,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    clear_folder(raw_data_directory)

    base_url = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
    current_time = datetime.strptime("000000", "%H%M%S")

    for _ in range(file_limit):
        filename = current_time.strftime("%H%M%SZ.json.gz")
        file_url = base_url + filename
        save_path = os.path.join(raw_data_directory, filename)
        fetch_gzip_file(file_url, save_path)

        current_time += timedelta(seconds=10)
        if current_time.second == 60:
            current_time = current_time.replace(second=0)

    return f"Downloaded {file_limit} files to {raw_data_directory}"


@s1.post("/aircraft/prepare")
def prepare_aircraft_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    clear_folder(prepared_data_directory)

    for file in os.listdir(raw_data_directory):
        print(f"Processing file: {file}")
        raw_file_path = os.path.join(raw_data_directory, file)
        prepared_file_path = os.path.join(prepared_data_directory, file.replace(".gz", ""))

        with open(raw_file_path, "r", encoding="utf-8") as raw_file:
            data = json.load(raw_file)
            timestamp = data["now"]
            aircraft_records = data["aircraft"]

            # Process the data to extract relevant fields
            processed_records = process_aircraft_data(aircraft_records, timestamp)

            # Drop records with null values for 'icao', 'registration', or 'type'
            filtered_records = [
                record for record in processed_records
                if record["icao"] and record["registration"] and record["type"]
            ]

            # Save the filtered and processed data
            with open(prepared_file_path, "w", encoding="utf-8") as prepared_file:
                json.dump(filtered_records, prepared_file)

            print(f"File processed and saved: {prepared_file_path}")

    return f"Prepared data saved to {prepared_data_directory}"



@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    aircraft_set = set()

    # Iterate over the prepared files and gather unique aircraft information
    for file in os.listdir(prepared_data_directory):
        with open(os.path.join(prepared_data_directory, file), "r", encoding="utf-8") as f:
            data = json.load(f)
            aircraft_set.update(
                (record["icao"], record.get("registration", "Unknown"), record.get("type", "Unknown"))
                for record in data if record["icao"]
            )

    # Sorting the rsults
    sorted_aircraft = sorted(list(aircraft_set), key=lambda x: x[0])
    start_index = page * num_results
    end_index = start_index + num_results

    return [
        {"icao": entry[0], "registration": entry[1], "type": entry[2]}
        for entry in sorted_aircraft[start_index:end_index]
    ]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_positions(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    positions_list = []

    # Search the prepared files for records matching the requested ICAO code
    for file in os.listdir(prepared_data_directory):
        with open(os.path.join(prepared_data_directory, file), "r", encoding="utf-8") as f:
            data = json.load(f)
            positions_list.extend(
                {
                    "timestamp": record.get("timestamp"),
                    "lat": record.get("lat"),
                    "lon": record.get("lon"),
                }
                for record in data if record["icao"] == icao
            )

    # Sorting the positions by timestamp 
    sorted_positions = sorted(positions_list, key=lambda x: x["timestamp"])
    start_index = page * num_results
    end_index = start_index + num_results
    return sorted_positions[start_index:end_index]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_stats(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    max_altitude = 0
    max_speed = 0
    emergency_status = False
    data_found = False

    # Looping to fins the specified flight
    for file in os.listdir(prepared_data_directory):
        file_path = os.path.join(prepared_data_directory, file)
        
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)

                
                for record in data:   # Searching for records matching the requested ICAO code
                    if record.get("icao", "").lower() == icao.lower():
                        data_found = True
                        altitude = record.get("max_altitude_baro", 0) # Check and update the maximum altitudee also the speed
                        speed = record.get("max_ground_speed", 0)    


                        altitude = altitude if altitude is not None else 0   # If no data, use 0 as the default value
                        speed = speed if speed is not None else 0                        
                        max_altitude = max(max_altitude, altitude) # Updating max values 


                        max_speed = max(max_speed, speed)  
                        emergency_status = emergency_status or record.get("had_emergency", False) # Check if an emergency was flagged 

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in {file_path}: {e}")
        except Exception as e:
            print(f"Unexpected error reading {file_path}: {e}")

    if not data_found:
        return {"error": f"No data found for ICAO {icao}"}


    # Returningg the collected statistics for the aircraft
    return {
        "max_altitude_baro": max_altitude,
        "had_emergency": emergency_status,
        "max_ground_speed": max_speed,
        
    }