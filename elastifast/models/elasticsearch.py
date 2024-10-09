from elasticsearch import Elasticsearch
from elastifast import settings

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
        self.client = Elasticsearch(
            hosts=[settings.elasticsearch_url],
            verify_certs=settings.elasticsearch_verify_certs,
            ca_certs=settings.elasticsearch_ssl_ca
        )
        self.myname = "elasticsearch"

    def __repr__(self) -> str:
        """
        Returns a string representation of the ElasticsearchClient object.

        Returns:
            str: The string representation.
        """
        return f"ElasticsearchClient(name={self.name})"