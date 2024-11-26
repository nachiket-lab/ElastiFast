import datetime
import requests
from typing import List, Optional, Tuple, Dict
from elastifast.config import logger
from elastifast.models.elasticsearch import ElasticsearchClient

# Constants
DEFAULT_LIMIT = 500


class AtlassianAPIClient:
    def __init__(self, org_id, secret_token: str):
        """
        Initialize the Atlassian API client.

        Args:
            secret_token (str): The API token for authorization.
        """
        self.org_id = org_id
        self.secret_token = secret_token
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.secret_token}",
        }

    @staticmethod
    def calculate_time_window(delta: int, 
                              current_time: Optional[datetime.datetime] = None
                              ) -> Tuple[int, int]:
        """
        Calculate the time window for querying.

        Args:
            delta (int): The time delta in minutes.
            current_time (datetime, optional): The reference time. Defaults to UTC now.

        Returns:
            Tuple[int, int]: Start and end times in milliseconds.
        """
        if current_time is None:
            current_time = datetime.datetime.now(datetime.timezone.utc)
        current_time = current_time.replace(second=0, microsecond=0)
        start_time = current_time - datetime.timedelta(minutes=delta * 2)
        end_time = current_time - datetime.timedelta(minutes=delta)
        return (
            round(start_time.timestamp() * 1000),
            round(end_time.timestamp() * 1000),
        )

    def build_url(self, time_delta: int, limit: int = DEFAULT_LIMIT) -> str:
        """
        Build the Atlassian API URL for fetching events.

        Args:
            time_delta (int): Time delta in minutes.
            limit (int): Maximum number of records to fetch per request.

        Returns:
            str: The constructed API URL.
        """
        start_time, end_time = self.calculate_time_window(time_delta)
        return (
            f"https://api.atlassian.com/admin/v1/orgs/{self.org_id}/events?from={start_time}&to={end_time}&limit={limit}"
        )

    def fetch_data(self, url: str) -> Optional[Dict]:
        """
        Fetch data from the Atlassian API.

        Args:
            url (str): The API endpoint URL.

        Returns:
            Optional[Dict]: The JSON response data or None if the request fails.
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error querying data from Atlassian: {e}")
            return None

    def get_events(self, time_delta: int = 5) -> List[Dict]:
        """
        Retrieve all events from the Atlassian API for the given time window.

        Args:
            org_id (str): Organization ID.
            time_delta (int): Time delta in minutes.

        Returns:
            List[Dict]: List of event records.
        """
        url = self.build_url(time_delta)
        self.data = []

        while url:
            logger.info(f"Fetching data from URL: {url}")
            result = self.fetch_data(url)
            if result and "data" in result:
                self.data.extend(result["data"])
                url = result.get("links", {}).get("next")
            else:
                logger.error("No data found or an error occurred.")
                raise ValueError("Data is empty or not found")

        logger.info(f"Fetched {len(self.data)} events from Atlassian.")
        return self.data
