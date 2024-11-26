from typing import List, Optional, Tuple, Dict
from elastifast.config import logger
from elastifast.models.apiclient import AbstractAPIClient

# Constants
DEFAULT_LIMIT = 1000
API_TIMEOUT = 30  # Timeout for API requests


class AtlassianAPIClient(AbstractAPIClient):
    """
    A client to interact with the Atlassian API for fetching organization events.

    Attributes:
        org_id (str): The organization ID.
        secret_token (str): The API token for authorization.
        headers (dict): Headers for API requests.
    """

    def __init__(self, org_id: str, secret_token: str, interval: int):
        """
        Initialize the Atlassian API client.

        Args:
            org_id (str): The organization ID.
            secret_token (str): The API token for authorization.
        """
        super().__init__(interval=interval, headers={"Authorization": f"Bearer {secret_token}"})
        self.org_id = org_id
        self.build_api_request()

    def build_api_request(self, limit: int = DEFAULT_LIMIT) -> str:
        """
        Build the Atlassian API URL for fetching events.

        Args:
            limit (int): Maximum number of records to fetch per request.

        Sets:
            self.url (str): The constructed API URL.
        """
        start_time = round(self.start_time.timestamp() * 1000)
        end_time = round(self.end_time.timestamp() * 1000)
        self.url = f"https://api.atlassian.com/admin/v1/orgs/{self.org_id}/events?from={start_time}&to={end_time}&limit={limit}"

    def get_events(self) -> List[Dict]:
        """
        Retrieve all events from the Atlassian API for the given time window.

        Args:
            time_delta (int): Time delta in minutes.

        Sets:
            self.data (List[Dict]): List of event records.
        """
        while self.url:
            logger.debug(f"Fetching data from URL: {self.url}")
            result = self.fetch_data()
            if result and "data" in result:
                self.data.extend(result["data"])
                self.url = result.get("links", {}).get("next")
            else:
                logger.warning("No more data to fetch or an error occurred.")
                self.url = None

        logger.info(f"Fetched {len(self.data)} events from Atlassian.")