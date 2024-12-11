from elastifast.models.apiclient import AbstractAPIClient
#from elastifast.config import logger

DEFAULT_LIMIT = 300
API_TIMEOUT = 30


class PostmanAuditLogIngestor(AbstractAPIClient):
    def __init__(self, secret_token: str, interval: int):
        super().__init__(
            interval=interval,
            headers={"Accept": "application/json", "X-Api-Key": secret_token},
        )
        self.build_api_request()

    def build_api_request(self):
        start_time = self.start_time.isoformat().split("+")[0]
        end_time = self.end_time.isoformat().split("+")[0]
        self.url = f"https://api.getpostman.com/audit/logs?since={start_time}&until={end_time}&limit={DEFAULT_LIMIT}"

    def get_events(self):
        while self.url:
            #logger.debug(f"Fetching data from Postman URL: {self.url}")
            result = self.fetch_data()
            self.data.extend(result.get("trails", []))
            self.url = result.get("nextCursor", None)
        return self.data
