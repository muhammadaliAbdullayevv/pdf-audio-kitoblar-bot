from dotenv import load_dotenv

from db import init_db, list_books, bulk_upsert_books
from bot import normalize


def main():
    load_dotenv()
    init_db()

    books = list_books()
    if not books:
        print("No books found in DB.")
        return

    updated = []
    for b in books:
        raw_name = (b.get("display_name") or b.get("book_name") or "").strip()
        if not raw_name:
            continue
        clean = normalize(raw_name)
        changed = False

        if b.get("book_name") != clean:
            b["book_name"] = clean
            changed = True
        if not b.get("display_name"):
            b["display_name"] = raw_name
            changed = True

        if changed:
            updated.append(b)

    if updated:
        bulk_upsert_books(updated)
    print(f"Books checked: {len(books)}")
    print(f"Books updated: {len(updated)}")


if __name__ == "__main__":
    main()
