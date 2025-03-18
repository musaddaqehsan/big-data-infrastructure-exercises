import json
import io
import gzip
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
import pytest

# Absolute imports from the bdi_api.s7 package
from bdi_api.s7.exercise import (
    s7,
    connect_to_database,
    create_database_tables,
    get_all_files_from_s3,
    save_to_database,
)
from bdi_api.app import app

# Attach router to app
app.include_router(s7)
client = TestClient(app)

# Sample data
SAMPLE_S3_DATA = [
    {
        "icao": "ABC123",
        "registration": "N123",
        "type": "Boeing 737",
        "lat": 40.7128,
        "lon": -74.0060,
        "altitude_baro": 10000,
        "ground_speed": 400,
        "emergency": False,
        "timestamp": 1698796800,
    },
    {
        "icao": "DEF456",
        "registration": "N456",
        "type": "Airbus A320",
        "lat": 34.0522,
        "lon": -118.2437,
        "altitude_baro": 15000,
        "ground_speed": 450,
        "emergency": True,
        "timestamp": 1698796805,
    },
]


def mock_s3_content():
    """Simulate gzipped JSON content for S3 mocking."""
    data = {"aircraft": SAMPLE_S3_DATA}
    json_str = json.dumps(data)
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(json_str.encode("utf-8"))
    buf.seek(0)
    return buf.read()


@pytest.fixture
def mock_db():
    """Mock database connection and cursor."""
    with patch("bdi_api.s7.exercise.psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("ABC123", "N123", "Boeing 737"),
            ("DEF456", "N456", "Airbus A320"),
        ]
        yield mock_conn, mock_cursor


@pytest.fixture
def mock_s3():
    """Mock S3 client and responses."""
    with patch("bdi_api.s7.exercise.boto3.client") as mock_client:
        mock_s3_client = MagicMock()
        mock_client.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "raw/day=20231101/000000Z.json.gz"}],
        }
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=lambda: mock_s3_content()),
        }
        yield mock_s3_client

def test_create_database_tables(mock_db):
    """Test database table creation."""
    conn, cursor = mock_db
    create_database_tables()
    cursor.execute.assert_called_once()
    conn.commit.assert_called_once()
    conn.close.assert_called_once()

def test_list_aircraft(mock_db):
    """Test listing aircraft with pagination."""
    response = client.get("/api/s7/aircraft/?num_results=2&page=0")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["icao"] == "ABC123"
    assert data[1]["icao"] == "DEF456"


def test_get_aircraft_position(mock_db):
    """Test retrieving aircraft positions."""
    conn, cursor = mock_db
    cursor.fetchall.return_value = [(1698796800, 40.7128, -74.0060)]
    response = client.get("/api/s7/aircraft/ABC123/positions?num_results=1&page=0")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0] == {"timestamp": 1698796800, "lat": 40.7128, "lon": -74.0060}


def test_get_aircraft_statistics(mock_db):
    """Test retrieving aircraft statistics."""
    conn, cursor = mock_db
    cursor.fetchone.return_value = (15000, 450, True)
    response = client.get("/api/s7/aircraft/DEF456/stats")
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "max_altitude_baro": 15000,
        "max_ground_speed": 450,
        "had_emergency": True,
    }