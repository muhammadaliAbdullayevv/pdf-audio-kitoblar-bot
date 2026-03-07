import os
import uuid

from dotenv import load_dotenv
from db import init_db, list_books, insert_book, find_duplicate_book
from bot import normalize

BOOKS_DIR = "/home/muhammadaliabdullayev/Documents/worldLibraryBot/downloads/localbooks"

def update_local_books():
    load_dotenv()
    init_db()
    books = list_books()
    existing_count = len(books)

    local_files = [
        f for f in os.listdir(BOOKS_DIR)
        if os.path.isfile(os.path.join(BOOKS_DIR, f))
    ]
    local_count = len(local_files)

    added_count = 0
    already_saved_count = 0
    skipped_names = []

    for filename in local_files:
        path = os.path.join(BOOKS_DIR, filename)
        display_name, _ = os.path.splitext(filename)
        display_name = display_name.strip()
        book_name = normalize(display_name)

        # ✅ Check duplicates using shared logic
        if find_duplicate_book(book_name, path, None):
            already_saved_count += 1
            skipped_names.append(book_name)
            continue

        # ✅ Assign permanent UUID and file_id = None
        new_book = {
            "id": str(uuid.uuid4()),
            "book_name": book_name,
            "display_name": display_name,
            "path": path,
            "file_id": None,   # not uploaded yet
            "file_unique_id": None,
            "indexed": False
        }
        ok = insert_book(new_book)
        if ok is False:
            already_saved_count += 1
            skipped_names.append(book_name)
            continue
        added_count += 1

    # 📊 Summary
    print("📊 Audit Summary")
    print(f"- Local books found: {local_count}")
    print(f"- Books already in DB: {already_saved_count}")
    print(f"- Books newly added to DB: {added_count}")
    print(f"- ✅ Total books in DB now: {existing_count + added_count}")

    if skipped_names:
        print("\n🔁 Already existing books (showing first 10):")
        for name in skipped_names[:10]:
            print(f" - {name}")

if __name__ == "__main__":
    print("🔎 Starting local book update...")
    update_local_books()
