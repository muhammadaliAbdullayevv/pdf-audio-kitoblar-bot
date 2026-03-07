import os
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv

load_dotenv(override=True)

ES_INDEX = "books"
from db import init_db, list_books

def main():
    es_url = os.getenv("ES_URL", "http://localhost:9200")
    es_ca_cert = os.getenv("ES_CA_CERT", "")
    es_user = os.getenv("ES_USER", "")
    es_pass = os.getenv("ES_PASS", "")

    kwargs = {}
    if es_ca_cert:
        kwargs["ca_certs"] = es_ca_cert
    if es_user and es_pass:
        kwargs["basic_auth"] = (es_user, es_pass)

    es = Elasticsearch(es_url, **kwargs)

    # --- 1. Delete existing index (wipe everything) ---
    if es.indices.exists(index=ES_INDEX):
        es.indices.delete(index=ES_INDEX)
        print(f"🗑️ Deleted existing index: {ES_INDEX}")

    # --- 2. Recreate index with settings/mappings ---
    es.indices.create(
        index=ES_INDEX,
        body={
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "-1"
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "book_name": {"type": "text"},
                    "display_name": {"type": "text"},
                    "file_id": {"type": "keyword"},
                    "file_unique_id": {"type": "keyword"},
                    "path": {"type": "text"},
                    "indexed": {"type": "boolean"}
                }
            }
        }
    )
    print(f"✅ Recreated index: {ES_INDEX}")

    # --- 3. Load books from DB ---
    init_db()
    books = list_books()

    # --- 4. Bulk index all books with UUID as ES _id ---
    def gen_actions():
        for b in books:
            book_id = b.get("id")
            if not book_id:
                continue  # skip if no UUID
            yield {
                "_op_type": "index",
                "_index": ES_INDEX,
                "_id": book_id,
                "id": book_id,
                "book_name": b.get("book_name"),
                "display_name": b.get("display_name") or b.get("book_name"),
                "file_id": b.get("file_id"),
                "file_unique_id": b.get("file_unique_id"),
                "path": b.get("path"),
                "indexed": b.get("indexed", True),
            }

    total = len([b for b in books if b.get("id")])
    if total:
        helpers.bulk(
            es,
            gen_actions(),
            chunk_size=1000,
            request_timeout=120,
            refresh=False,
        )
        # restore normal refresh + force refresh once
        es.indices.put_settings(index=ES_INDEX, body={"index": {"refresh_interval": "1s"}})
        es.indices.refresh(index=ES_INDEX)
        print(f"📚 Bulk indexed {total} books into ES")
    else:
        print("⚠️ No valid books found in DB")

if __name__ == "__main__":
    main()
