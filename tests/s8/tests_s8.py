import pytest
import requests
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from datetime import datetime
from bdi_api.s8.exercise import s8, SessionLocal, FUEL_CONSUMPTION_URL
from fastapi import FastAPI

app = FastAPI()
app.include_router(s8)

client = TestClient(app)

mock_aircraft_db_model = "Airbus A320"

mock_fuel_data = {
    "Airbus A320": {"galph": 800},
}

@pytest.fixture
def mock_session():
    with patch("bdi_api.s8.exercise.SessionLocal") as mock_session_local:
        mock_session_instance = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session_instance
        yield mock_session_instance

@pytest.fixture
def mock_requests_get():
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = mock_fuel_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        yield mock_get

def test_list_aircraft_pagination(mock_session):
    mock_session.execute.return_value.all.return_value = []
    response = client.get("/api/s8/aircraft/?num_results=2&page=1")
    assert response.status_code == 200
    assert response.json() == []

def test_get_aircraft_co2_success(mock_session, mock_requests_get):
    mock_session.execute.side_effect = [
        MagicMock(scalar=lambda: 720),
        MagicMock(scalar=lambda: mock_aircraft_db_model)
    ]
    response = client.get("/api/s8/aircraft/0101e0/co2?day=2023-11-01")
    assert response.status_code == 200
    result = response.json()
    assert result["icao"] == "0101e0"
    assert result["hours_flown"] == 1.0
    assert abs(result["co2"] - 8.44) < 0.01

def test_get_aircraft_co2_invalid_date_format(mock_session):
    response = client.get("/api/s8/aircraft/0101e0/co2?day=2023-13-01")
    assert response.status_code == 422
    assert response.json()["detail"] == "Invalid day format. Use YYYY-MM-DD"

def test_get_aircraft_co2_invalid_day(mock_session):
    response = client.get("/api/s8/aircraft/0101e0/co2?day=2023-11-02")
    assert response.status_code == 422
    assert response.json()["detail"] == "Data only available for the 1st of each month"

def test_get_aircraft_co2_aircraft_not_found(mock_session):
    mock_session.execute.side_effect = [
        MagicMock(scalar=lambda: 0),
        MagicMock(scalar=lambda: None)
    ]
    response = client.get("/api/s8/aircraft/unknown/co2?day=2023-11-01")
    assert response.status_code == 404
    assert response.json()["detail"] == "Aircraft not found"

def test_get_aircraft_co2_fuel_data_unavailable(mock_session, mock_requests_get):
    mock_session.execute.side_effect = [
        MagicMock(scalar=lambda: 720),
        MagicMock(scalar=lambda: mock_aircraft_db_model)
    ]
    mock_requests_get.side_effect = requests.RequestException("Failed to fetch fuel data")
    response = client.get("/api/s8/aircraft/0101e0/co2?day=2023-11-01")
    assert response.status_code == 200
    result = response.json()
    assert result["icao"] == "0101e0"
    assert result["hours_flown"] == 1.0
    assert result["co2"] is None