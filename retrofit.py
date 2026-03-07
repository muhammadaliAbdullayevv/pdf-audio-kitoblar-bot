import uuid
from db import init_db, list_books, insert_book
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

def retrofit_books_with_uuid():
    load_dotenv()
    init_db()
    books = list_books()
    updated = False
    for book in books:
        if "id" not in book or not book["id"]:
            book["id"] = str(uuid.uuid4())
            insert_book(book)
            updated = True
    if updated:
        logger.info("✅ Added UUIDs in DB for missing entries.")
    else:
        logger.info("All books already have UUIDs.")

if __name__ == "__main__":
    retrofit_books_with_uuid()
