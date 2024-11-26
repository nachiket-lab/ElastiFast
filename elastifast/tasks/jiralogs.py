from elastifast.config import logger
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta, timezone
import requests
import re

class JiraAuditLogIngestor:
    def __init__(self, jira_url, username, api_key):
        self.jira_url = jira_url
        self.auth = HTTPBasicAuth(username, api_key)
        self.headers = {"Accept": "application/json"}
        self.records = []
    
    def _get_time_range(self, delta=5):
        ####    
        self.time = datetime.now(timezone.utc)
        time = self.time.replace(second=0, microsecond=0)
        start_time = time - timedelta(minutes=delta * 2)
        end_time = time - timedelta(minutes=delta)
        to_time = end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "+0000"
        from_time = start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "+0000"
        logger.debug(f"Jira logs puller - From time: {from_time}, To time: {to_time}")
        return from_time, to_time
    
    def get_events(self, interval_minutes=5, max_results=1000):
        from_time, to_time = self._get_time_range(interval_minutes)
        start_at = 0

        while True:
            params = {
                "offset": start_at,
                "limit": max_results,
                "from": from_time,
                "to": to_time
            }
            response = requests.get(self.jira_url, headers=self.headers, auth=self.auth, params=params)
            
            if response.status_code != 200:
                logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")
                break

            data = response.json()
            self.records.extend(data.get('records', []))

            if start_at + max_results >= data.get('total', 0):
                logger.info(f"Fetched {len(self.records)} records from Jira")
                break

            start_at += max_results
        
        self.records = self._prepare_records()

    def _prepare_records(self):
        pattern = r'\"(.*?)\"'
        formatted_data = []

        for data in self.records:
            try:
                # Modify records based on criteria
                if data.get("summary") == "Custom field created" and "changedValues" in data:
                    del data["changedValues"]

                formatted_record = {
                    "@timestamp": self.time.isoformat(),
                    "message": re.sub(pattern, r"'\1'", str(data).replace('"', "'")).replace("'{", '"{').replace("}'", '}"')
                }
                formatted_data.append(formatted_record)
            except Exception as e:
                logger.info(f"Error processing record: {e}")

        return formatted_data
