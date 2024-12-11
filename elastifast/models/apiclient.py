from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from math import e
from tracemalloc import start
from typing import Dict, List, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth

from elastifast.config.logging import logger


class AbstractAPIClient(ABC):

    def __init__(
        self,
        interval: int = None,
        base_url: str = None,
        headers: dict = None,
        username: str = None,
        password: str = None,
        params=None,
        start_time=None,
        end_time=None,
    ):
        self.current_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        self.interval = interval
        if self.interval:
            self.start_time, self.end_time = self.calculate_time_window()
        elif start_time and end_time:
            self.start_time, self.end_time = datetime.fromisoformat(
                start_time, timezone=timezone.utc
            ), datetime.fromisoformat(end_time, timezone=timezone.utc)
        else:
            raise ValueError("interval or start_time and end_time must be provided ")
        self.url = base_url
        self.data = []
        self.headers = {"Accept": "application/json", **(headers or {})}
        if username and password:
            self.auth = HTTPBasicAuth(username, password)
        else:
            self.auth = None
        self.params = params

    def calculate_time_window(self) -> Tuple[str, str]:
        start_time = self.current_time - timedelta(minutes=self.interval * 2)
        end_time = self.current_time - timedelta(minutes=self.interval)
        return (start_time, end_time)

    def fetch_data(self, API_TIMEOUT: int = 10) -> Optional[Dict]:
        """
        Fetch data from the provided API.

        Args:
            url (str): The API endpoint URL.

        Returns:
            Optional[Dict]: The JSON response data or None if the request fails.
        """
        try:
            response = requests.get(
                self.url,
                headers=self.headers,
                timeout=API_TIMEOUT,
                auth=self.auth,
                params=self.params,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error querying data from {self.__class__.__name__}: {e}")
            raise

    @abstractmethod
    def build_api_request(self, **kwargs) -> str:
        pass

    @abstractmethod
    def get_events(self) -> List[Dict]:
        pass

    @property
    def message(self):
        if len(self.data) > 0:
            return (
                f"Data ingested from {self.__class__.__name__} {len(self.data)} events"
            )
        else:
            return f"No data to ingest from {self.__class__.__name__}"
