from elasticsearch import Elasticsearch
from elastifast.config import settings


class ElasticsearchClient(object):
    """
    A class representing an Elasticsearch client.

    Args:
        settings (Settings): The settings for the Elasticsearch client.
    """

    def __init__(self) -> None:
        """
        Initializes the Elasticsearch client.

        Args:
            settings (Settings): The settings for the Elasticsearch client.

        Raises:
            ConnectionError: If the Elasticsearch client cannot be created.
        """
        self.client = self._create_elasticsearch_client()

    def _create_elasticsearch_client(self):
        auth_kwargs = {}
        if settings.elasticsearch_auth_method == "basic":
            auth_kwargs = {
                "http_auth": (
                    settings.elasticsearch_username,
                    settings.elasticsearch_password,
                )
            }
        elif settings.elasticsearch_auth_method == "api_key":
            auth_kwargs = {
                "api_key": (
                    settings.elasticsearch_api_key_id,
                    settings.elasticsearch_api_key,
                )
            }

        return Elasticsearch(
            hosts=[settings.elasticsearch_url],
            verify_certs=settings.elasticsearch_verify_certs,
            ca_certs=settings.elasticsearch_ssl_ca,
            **auth_kwargs
        )
