import datetime
import requests
from typing import List, Optional, Tuple, Dict
from elastifast.config import logger
from elastifast.models.elasticsearch import ElasticsearchClient

# Constants
DEFAULT_LIMIT = 300

class postmanauditlogger:
    def __init__(self, secret_token: str):
        """
        Initialize the Postman API client.

        Args:
            secret_token (str): The API token for authorization.
        """
        self.secret_token = secret_token
        self.headers = {
            "Accept": "application/json",
            "X-API-Key": f"{self.secret_token}",
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
            Tuple[str, str]: Start and end times in ISO 8601 Format.
        """
        if current_time is None:
            current_time = datetime.datetime.now(datetime.timezone.utc)
        current_time = current_time.replace(second=0, microsecond=0)
        start_time = current_time - datetime.timedelta(minutes=delta * 2)
        end_time = current_time - datetime.timedelta(minutes=delta)
        return (start_time,end_time)
    
    def build_url(self, time_delta: int, limit: int = DEFAULT_LIMIT) -> str:
        """
        Build the Postman API URL for fetching events.

        Args:
            time_delta (int): Time delta in minutes.
            limit (int): Maximum number of records to fetch per request.

        Returns:
            str: The constructed API URL.
        """
        start_time, end_time = self.calculate_time_window(time_delta)
        return (
            f"https://api.getpostman.com/audit/logs?since={start_time}&until={end_time}&limit={limit}"
        )
    
    def fetch_data(self, url: str) -> Optional[Dict]:
        """
        Fetch data from the Postman API.

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
            logger.error(f"Error querying data from Postman: {e}")
            return None
        
    def get_events(self, time_delta: int = 5) -> List[Dict]:
        """
        Retrieve all events from the Postman API for the given time window.

        Args:
            time_delta (int): Time delta in minutes.

        Returns:
            List[Dict]: List of event records.
        """
        url = self.build_url(time_delta)
        self.data = []

        while url:
            logger.info(f"Fetching data from URL: {url}")
            result = self.fetch_data(url)
            if result and "trails" in result:
                self.data.extend(result["trails"])
                url = result.get("nextCursor")
            else:
                logger.error("No data found or an error occurred.")
                raise ValueError("Data is empty or not found")

        logger.info(f"Fetched {len(self.data)} events from Postman.")
        return self.data
