from typing import Optional
from urllib.parse import quote
import ast
import yaml
from elasticapm.contrib.starlette import ElasticAPM, make_apm_client
from pydantic import AnyUrl, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings
from typing_extensions import Self
from elastifast.config.logging import logger

# Define a base settings class with validationfrom pydantic import BaseSettings, AnyUrl
class Settings(BaseSettings):
    # db_host: str
    # db_port: int
    # db_username: str
    # db_password: str
    elasticsearch_host: str
    elasticsearch_port: int
    elasticsearch_auth_method: str = "basic"  # or "api_key"
    elasticsearch_username: Optional[str] = None
    elasticsearch_password: Optional[str] = None
    elasticsearch_api_key_id: Optional[str] = None
    elasticsearch_api_key: Optional[str] = None
    elasticsearch_compress: Optional[bool] = True
    elasticsearch_ssl_enabled: Optional[bool] = True
    elasticsearch_ssl_ca: Optional[str] = None
    elasticsearch_verify_certs: Optional[bool] = True
    celery_broker_url: AnyUrl
    celery_broker_transport_options: Optional[dict] = None
    # celery_result_backend: AnyUrl
    elasticapm_service_name: Optional[str] = "elastifast"
    elasticapm_server_url: Optional[AnyUrl] = None
    elasticapm_es_url: AnyUrl
    elasticapm_secret_token: Optional[str] = None
    elasticsearch_celery_username: Optional[str] = None
    elasticsearch_celery_password: Optional[str] = None
    elasticapm_environment: Optional[str] = "production"
    atlassian_org_id: Optional[str] = None
    atlassian_secret_token: Optional[str] = None
    jira_url: Optional[AnyUrl] = None
    jira_username: Optional[str] = None
    jira_api_key: Optional[str] = None
    zendesk_username: Optional[str] = None
    zendesk_api_key: Optional[str] = None
    zendesk_tenant: Optional[str] = None
    postman_secret_token: Optional[str] = None
    celery_beat_schedule: Optional[bool] = False
    celery_beat_interval: Optional[int] = 5

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def apm_client(self):
        if (
            self.elasticapm_service_name
            and self.elasticapm_server_url
            and self.elasticapm_secret_token
        ):
            client = make_apm_client(
                {
                    "SERVICE_NAME": self.elasticapm_service_name,
                    "SERVER_URL": self.elasticapm_server_url,
                    "SECRET_TOKEN": self.elasticapm_secret_token,
                    "ENVIRONMENT": self.elasticapm_environment,
                }
            )
            try:
                import celery
                from elasticapm.contrib.celery import (
                    register_exception_tracking, register_instrumentation)

                register_instrumentation(client)
                register_exception_tracking(client)
                logger.info("ElasticAPM initialized with Celery")
                return client
            except ImportError:
                logger.info("Celery not found. Skipping Celery instrumentation")
        else:
            logger.error("APM client not initialized")
            return None

    @property
    def celery_result_backend(self) -> AnyUrl:
        if self.elasticsearch_celery_username and self.elasticsearch_celery_password:
            creds = f"{self.elasticsearch_celery_username}:{quote(self.elasticsearch_celery_password)}"
        elif self.elasticsearch_username and self.elasticsearch_password:
            creds = (
                f"{self.elasticsearch_username}:{quote(self.elasticsearch_password)}"
            )
        else:
            raise ValueError("Missing credentials for ElasticAPM server")
        return f"elasticsearch+{self.elasticapm_es_url.scheme}://{creds}@{self.elasticapm_es_url.host}:{self.elasticapm_es_url.port}/{self.celery_index_name}"

    @property
    def elasticsearch_url(self) -> AnyUrl:
        scheme = "https" if self.elasticsearch_ssl_enabled else "http"
        return f"{scheme}://{self.elasticsearch_host}:{self.elasticsearch_port}"

    @field_validator("elasticsearch_auth_method")
    def validate_auth_method(cls, value):
        if value not in ["basic", "api_key"]:
            raise ValueError(
                "Invalid authentication method. Must be 'basic' or 'api_key'."
            )
        return value

    @model_validator(mode="after")
    def validate_auth_credentials(self) -> Self:
        if self.elasticsearch_auth_method == "basic" and (
            not self.elasticsearch_username or not self.elasticsearch_password
        ):
            raise ValueError(
                "Username and password are required for basic authentication."
            )
        elif self.elasticsearch_auth_method == "api_key" and (
            not self.elasticsearch_api_key_id or not self.elasticsearch_api_key
        ):
            raise ValueError(
                "API key ID and API key are required for API key authentication."
            )
        return self

    @field_validator("elasticsearch_host")
    def validate_elasticsearch_host(cls, value):
        if not value:
            raise ValueError("elasticsearch_host is required")
        if value.startswith("http://") or value.startswith("https://"):
            raise ValueError(
                "elasticsearch_host must be a hostname or IP address, not a URL"
            )
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

    @field_validator("celery_broker_url")
    def validate_celery_broker_url(cls, value):
        if not value.scheme in ["redis", "amqp", "amqps", "sqs"]:
            raise ValueError(
                "Invalid Celery broker URL. Must be one of: redis, amqp, amqps, sqs."
            )
        return value
    
    @field_validator("celery_broker_transport_options", mode="before")
    def validate_celery_broker_transport_options(cls, value):
        if isinstance(value, str):
            try:
                return ast.literal_eval(value)
            except (ValueError, SyntaxError):
                raise ValueError("Invalid JSON format for Celery broker transport options")
        else:
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

settings = load_settings()