from elasticsearch.helpers import bulk

def index_data(esclient, data: list, index_name: str):
    for item in data:
        item["_index"] = index_name
    res = bulk(es, data)
    return res