from os.path import dirname, join
import os
from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

import bdi_api

PROJECT_DIR = dirname(dirname(bdi_api.__file__))

# Load environment variables from .env at the top
load_dotenv()

class DBCredentials(BaseSettings):
    host: str
    port: int = 5432
    database: str
    username: str
    password: str

    model_config = SettingsConfigDict(
        env_prefix="bdi_db_",  # Matches your .env file
        env_file=".env",       # Explicitly load from .env
        env_file_encoding="utf-8",
        extra="ignore"         # Ignore extra env vars not in this model
    )

db_credentials = DBCredentials()

class Settings(BaseSettings):
    source_url: str = Field(
        default="https://samples.adsbexchange.com/readsb-hist",
        description="Base URL to the website used to download the data.",
    )
    local_dir: str = Field(
        default=join(PROJECT_DIR, "data"),
        description="For any other value set env variable 'BDI_LOCAL_DIR'",
    )
    s3_bucket: str = Field(
        default="bdi-test",
        description="Call the api like `BDI_S3_BUCKET=yourbucket poetry run uvicorn...`",
    )

    telemetry: bool = False
    telemetry_dsn: str = "http://project2_secret_token@uptrace:14317/2"

    model_config = SettingsConfigDict(
        env_prefix="bdi_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"  # Ignore extra env vars like bdi_db_*
    )

    @property
    def raw_dir(self) -> str:
        """Store inside all the raw jsons"""
        return join(self.local_dir, "raw")

    @property
    def prepared_dir(self) -> str:
        return join(self.local_dir, "prepared")