from operator import ne

from elastifast.config import logger
from elastifast.models.apiclient import AbstractAPIClient

DEFAULT_LIMIT = 1000


class ZendeskAuditLogIngestor(AbstractAPIClient):
    def __init__(self, secret_token: str, interval: int, username: str, api_key: str):
        username = username + "/token"
        password = api_key
        super().__init__(
            interval=interval,
            username=username,
            password=password,
        )
        self.build_api_request()

    def build_api_request(self):
        self.url = f"https://helpdesk.zendesk.com/api/v2/audit_logs.json?filter[created_at]={self.start_time}&filter[created_at]={self.end_time}&page[size]={DEFAULT_LIMIT}"

    def get_events(self):
        while self.url:
            result = self.fetch_data()
            self.data.extend(result.get("audit_logs", []))
            next_url = result.get("links", None)
            if next_url:
                self.url = next_url.get("next", None)
            else:
                self.url = None
                logger.warn("No more data to fetch.")
        logger.info(f"Fetched {len(self.data)} events from Zendesk.")
