from email import message
from elasticsearch.helpers import BulkIndexError, bulk

from elastifast.config import logger


class ElasticsearchIngestData:
    def __init__(self, esclient, data: list, index_name: str):
        self.esclient = esclient
        self.run()

    def _prep_data(self):
        for item in self.data:
            item["_index"] = self.index_name
            item["_op_type"] = "create"
    
    def run(self):
        try:
            self._prep_data()
        except Exception as e:
            self.message = f"Error of type {type(e)} occured while prepping the data: {e}."
            raise
        try:
            res = bulk(self.esclient, self.data)
            self.message = f"Data ingested by {self.esclient.__class__.__name__}:  success={len(res[0])} events, failure={len(res[1])} events"
        except BulkIndexError as e:
            self.message = f"Indexing error while ingesting data: {e.errors}."
            logger.error(message)
            raise
        except Exception as e:
            self.message = f"Error of type {type(e)} occured while ingesting data: {e}."
            logger.error(message)
            raise