import json
import os
import sys
from datetime import datetime, date

from dotenv import load_dotenv
from db import (
    init_db,
    upsert_user,
    bulk_upsert_books,
    add_favorite,
    add_recent,
    insert_request,
    insert_upload_request,
    db_conn,
)

MAX_FAVORITES = 50
MAX_RECENTS = 5


def parse_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except Exception:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except Exception:
            return None


def main(reset: bool = False):
    load_dotenv()
    init_db()
    if reset:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE user_favorites, user_recents, book_requests, upload_requests, analytics_daily, books, users RESTART IDENTITY")

    if os.path.exists("users.json"):
        users = json.load(open("users.json", "r", encoding="utf-8"))
        for u in users:
            joined = parse_date(u.get("joined_date"))
            left = parse_date(u.get("left_date"))
            upsert_user(
                user_id=u.get("id"),
                username=u.get("username"),
                first_name=u.get("first_name"),
                last_name=u.get("last_name"),
                blocked=bool(u.get("blocked", False)),
                allowed=bool(u.get("allowed", False)),
                joined_date=joined,
                left_date=left,
                language=u.get("language"),
                language_selected=u.get("language_selected"),
            )
            # favorites/recents if present
            for fav in u.get("favorites", []) or []:
                if fav.get("id"):
                    add_favorite(u.get("id"), fav.get("id"), fav.get("title", ""), MAX_FAVORITES)
            for rec in u.get("recent", []) or []:
                if rec.get("id"):
                    add_recent(u.get("id"), rec.get("id"), rec.get("title", ""), MAX_RECENTS)

    if os.path.exists("books.json"):
        books = json.load(open("books.json", "r", encoding="utf-8"))
        bulk_upsert_books(books)

    if os.path.exists("analytics.json"):
        analytics = json.load(open("analytics.json", "r", encoding="utf-8"))
        with db_conn() as conn:
            with conn.cursor() as cur:
                for day, vals in analytics.items():
                    cur.execute(
                        """
                        INSERT INTO analytics_daily (day, searches, buttons)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (day) DO UPDATE SET
                            searches=EXCLUDED.searches,
                            buttons=EXCLUDED.buttons
                        """,
                        (day, int(vals.get("searches", 0)), int(vals.get("buttons", 0))),
                    )

    if os.path.exists("requests.json"):
        reqs = json.load(open("requests.json", "r", encoding="utf-8"))
        for r in reqs:
            insert_request(r)

    if os.path.exists("upload_requests.json"):
        reqs = json.load(open("upload_requests.json", "r", encoding="utf-8"))
        for r in reqs:
            insert_upload_request(r)


if __name__ == "__main__":
    reset = "--reset" in sys.argv
    main(reset=reset)
    print("Migration complete.")
