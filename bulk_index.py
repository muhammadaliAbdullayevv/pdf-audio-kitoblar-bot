from elasticsearch import Elasticsearch, helpers
import os
from dotenv import load_dotenv

from db import init_db, list_books

load_dotenv()

ES_URL = os.getenv("ES_URL", "https://localhost:9200")
ES_CA_CERT = os.getenv("ES_CA_CERT", "")
ES_USER = os.getenv("ES_USER", "")
ES_PASS = os.getenv("ES_PASS", "")

kwargs = {}
if ES_CA_CERT:
    kwargs["ca_certs"] = ES_CA_CERT
if ES_USER and ES_PASS:
    kwargs["basic_auth"] = (ES_USER, ES_PASS)

es = Elasticsearch(ES_URL, **kwargs)

ES_INDEX = "books"

# Load books from DB
init_db()
books = list_books()

# Prepare bulk actions using existing UUIDs
actions = [
    {
        "_index": ES_INDEX,
        "_id": book["id"],   # ✅ reuse UUID from DB
        "_source": book
    }
    for book in books
]

# Bulk index in chunks
helpers.bulk(es, actions, chunk_size=1000, refresh="wait_for")

print(f"✅ Bulk indexed {len(actions)} books into Elasticsearch")
