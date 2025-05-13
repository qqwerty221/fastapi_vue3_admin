
from pymilvus import MilvusClient

client = MilvusClient(
    uri="https://datam-sit.cmsk1979.com:443",
    token="root:Milvus",
    db_name="default"
)
