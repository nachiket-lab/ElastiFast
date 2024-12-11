import re
from tracemalloc import start
from typing import Dict, List, Tuple

import requests

from elastifast.config import logger
from elastifast.models.apiclient import AbstractAPIClient

DEFAULT_LIMIT = 10000


class JiraAuditLogIngestor(AbstractAPIClient):
    """
    A class to fetch and process Jira audit logs.

    Attributes:
        url (str): Base URL for the Jira API.
        auth (HTTPBasicAuth): Authentication object for Jira API.
        headers (dict): Headers for Jira API requests.
        records (list): List of fetched and processed records.
        current_time (datetime): The current timestamp used for time range calculations.
    """

    def __init__(self, interval: int, url: str, username: str, password: str):
        """
        Initialize JiraAuditLogIngestor.

        Args:
            url (str): The base URL for the Jira API.
            username (str): Username for Jira API authentication.
            api_key (str): API key for Jira API authentication.
        """
        super().__init__(interval=interval, base_url=url, username=username, password=password)
        self.build_api_request()
        self._data = []

    def build_api_request(self):
        self._from_time = (
            self.start_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+0000"
        )
        self._to_time = self.end_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+0000"
        logger.debug(
            f"Jira logs puller - From time: {self._from_time}, To time: {self._to_time}"
        )

    def get_events(self):
        start_at = 0

        while True:
            print("Entering loop for get events")
            self.params = {
                "offset": start_at,
                "limit": DEFAULT_LIMIT,
                "from": self._from_time,
                "to": self._to_time,
            }
            try:
                data = self.fetch_data()
                print(f"loop for get events {data.get('total', 0)} exist. Records: {len(data.get("records", 0))}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching records: {e}")
                raise

            self._data.extend(data.get("records", []))

            if start_at + DEFAULT_LIMIT >= data.get("total", 0):
                logger.info(f"Fetched {len(self._data)} records from Jira")
                break

            start_at += DEFAULT_LIMIT    
        self.data = self._prepare_records()

    def _format_record(self, data: Dict) -> Dict:
        """
        Format a single Jira log record for processing.

        Args:
            data (dict): A single Jira log record.

        Returns:
            dict: A formatted log record.
        """
        try:
            if data.get("summary") == "Custom field created":
                data.pop("changedValues", None)  # Safely remove the key if it exists

            pattern = r"\"(.*?)\""
            formatted_message = re.sub(pattern, r"'\1'", str(data).replace('"', "'"))
            formatted_message = formatted_message.replace("'{", '"{').replace(
                "}'", '}"'
            )

            return {
                "@timestamp": self.current_time.isoformat(),
                "message": formatted_message,
            }
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            return {}

    def _prepare_records(self) -> List[Dict]:
        """
        Process and format all fetched records.

        Returns:
            list: A list of formatted records.
        """
        return [self._format_record(record) for record in self._data if record]
