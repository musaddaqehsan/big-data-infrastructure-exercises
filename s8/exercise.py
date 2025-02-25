from typing import Optional

from fastapi import APIRouter, status
from pydantic import BaseModel

from bdi_api.settings import DBCredentials, Settings

settings = Settings()
db_credentials = DBCredentials()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)


class AircraftReturn(BaseModel):
    # DO NOT MODIFY IT
    icao: str
    registration: Optional[str]
    type: Optional[str]
    owner: Optional[str]
    manufacturer: Optional[str]
    model: Optional[str]


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    ADDITIONS:
    * Instead of passing a JSON, use pydantic to return the correct schema
       See: https://fastapi.tiangolo.com/tutorial/response-model/
    * Enrich it with information from the aircrafts database (see README for link)
      * `owner`  (`ownop` field in the aircrafts DB)
      * `manufacturer` and `model`


    IMPORTANT: Only return the aircraft that we have seen and not the entire list in the aircrafts database

    """
    # TODO
    return [
        AircraftReturn.parse_obj(
            {
                "icao": "a835af",
                "registration": "N628TS",
                "type": "GLF6",
                "manufacturer": "GULFSTREAM AEROSPACE CORP",
                "model": "GVI (G650ER)",
                "owner": "Elon Musk",
            }
        ),
    ]


class AircraftCO2(BaseModel):
    # DO NOT MODIFY IT
    icao: str
    hours_flown: float
    """Co2 tons generated"""
    co2: Optional[float]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2:
    """Returns the CO2 generated by the aircraft **in a given day**.

    Compute the hours flown by the aircraft (assume each row we have is 5s).

    Then, you can use these metrics:

    ```python
    fuel_used_kg = fuel_used_gal * 3.04
        c02_tons = (fuel_used_kg * 3.15 ) / 907.185
        ```

    Use the gallon per hour from https://github.com/martsec/flight_co2_analysis/blob/main/data/aircraft_type_fuel_consumption_rates.json
    The key is the `icaotype`

    ```json
    {
      ...,
      "GLF6": { "source":"https://github.com/Jxck-S/plane-notify",
        "name": "Gulfstream G650",
        "galph": 503,
        "category": "Ultra Long Range"
      },
    }

    If you don't have the fuel consumption rate, return `None` in the `co2` field
    ```
    """
    # TODO
    day_to_compute = day
    return AircraftCO2(icao=icao, hours_flown=12.2, co2=1.5)
