from tracemalloc import start
from elastifast.config.logging import logger
from elastifast.models.apiclient import AbstractAPIClient

# Do not change to more than 100, will lead to BAD REQUEST
DEFAULT_LIMIT = 100

class ZendeskAuditLogIngestor(AbstractAPIClient):
    def __init__(self, interval: int, username: str, api_key: str, tenant: str):
        username = username + "/token"
        password = api_key
        self.tenant = tenant
        super().__init__(
            interval=interval,
            username=username,
            password=password,
        )
        self.build_api_request()

    def build_api_request(self):
        start_time = self.start_time.isoformat().split("+")[0]
        end_time = self.end_time.isoformat().split("+")[0]
        self.url = f"https://{self.tenant}.zendesk.com/api/v2/audit_logs.json?filter[created_at][]={start_time}Z&filter[created_at][]={end_time}Z&page[size]={DEFAULT_LIMIT}"

    def get_events(self):
        while self.url:
            result = self.fetch_data()
            self.data.extend(result.get("audit_logs", []))
            next_url = result.get("links", None)
            self.url = next_url.get("next", None)
            if self.url is None:
                logger.warning("No more data to fetch.")
        logger.info(f"Fetched {len(self.data)} events from Zendesk.")
