import os
import yaml
from pydantic import BaseSettings, ValidationError, AnyUrl, field_validator
from fastapi import FastAPI
from celery import Celery
from elasticsearch import Elasticsearch
from elasticapm.contrib.starlette import make_apm_client, ElasticAPM
from starlette.applications import Starlette
import logging
import ecs_logging


def create_ecs_logger():
    """
    Creates a logger that logs messages in ECS format (https://www.elastic.co/guide/en/ecs/current/index.html).

    Returns:
        logging.Logger: The ecs logger.
    """
    alogger = logging.getLogger(__name__)
    alogger.setLevel(logging.INFO)

    # Configure the logger to use ECS formatter
    handler = logging.StreamHandler()
    handler.setFormatter(ecs_logging.StdlibFormatter())
    alogger.addHandler(handler)

    return alogger


logger = create_ecs_logger()

# Define a base settings class with validationfrom pydantic import BaseSettings, AnyUrl


class Settings(BaseSettings):
    db_host: str
    db_port: int
    db_username: str
    db_password: str
    elasticsearch_host: str
    elasticsearch_port: int
    elasticsearch_compress: bool = True
    elasticsearch_ssl_enabled: bool = True
    elasticsearch_ssl_verify: bool = True
    elasticsearch_ssl_ca: str = None
    elasticsearch_verify_certs: bool = True
    celery_broker_url: str
    celery_result_backend: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def elasticsearch_url(self) -> AnyUrl:
        scheme= "https" if self.elasticsearch_ssl_enabled else "http"
        return f"{scheme}://{self.elasticsearch_host}:{self.elasticsearch_port}"

    @property
    def elasticsearch_ssl_params(self) -> dict:
        ssl_params = {}
        if self.elasticsearch_ssl_enabled:
            ssl_params = {
                "ssl_cert": self.elasticsearch_ssl_cert,
                "ssl_key": self.elasticsearch_ssl_key,
                "ssl_ca": self.elasticsearch_ssl_ca,
                "verify_certs": self.elasticsearch_verify_certs,
            }
        return ssl_params
    
    @field_validator("elasticsearch_host")
    def validate_elasticsearch_host(cls, value):
        if not value:
            raise ValueError("elasticsearch_host is required")
        if value.startswith("http://") or value.startswith("https://"):
            raise ValueError("elasticsearch_host must be a hostname or IP address, not a URL")
        return value

    @field_validator("elasticsearch_port")
    def validate_elasticsearch_port(cls, value):
        if not value:
            raise ValueError("elasticsearch_port is required")
        if not isinstance(value, int):
            raise ValueError("elasticsearch_port must be an integer")
        if value < 0 or value > 65535:
            raise ValueError("elasticsearch_port must be between 0 and 65535")
        return value

# Load settings from YAML file or environment variables
def load_settings() -> Settings:
    try:
        # Try to load settings from YAML file
        with open("settings.yaml", "r") as f:
            settings = Settings.parse_obj(yaml.safe_load(f))
        return settings
    except FileNotFoundError:
        try:
            # If YAML file not found, try to load settings from environment variables
            settings = Settings()
            return settings
        except ValidationError as e:
            logger.error(f"Error loading settings: {e}")
            raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML: {e}")
        raise
