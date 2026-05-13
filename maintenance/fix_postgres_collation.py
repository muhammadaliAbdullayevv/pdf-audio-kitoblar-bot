#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

import psycopg2
from psycopg2 import sql

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db


def _connect(*, dbname: str | None = None, connect_timeout: int = 5, autocommit: bool = True):
    params = dict(db._dsn())
    if dbname:
        params["dbname"] = dbname
    params["connect_timeout"] = int(connect_timeout)
    conn = psycopg2.connect(**params)
    conn.autocommit = autocommit
    return conn


def _close(conn) -> None:
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass


def _current_database(connect_timeout: int) -> str:
    stats = db.ping_db()
    if not stats.get("ok"):
        raise RuntimeError(str(stats.get("error") or "database unavailable"))
    name = str(stats.get("database") or "").strip()
    if not name:
        conn = _connect(connect_timeout=connect_timeout)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                row = cur.fetchone()
                name = str(row[0] if row else "").strip()
        finally:
            _close(conn)
    if not name:
        raise RuntimeError("could not resolve current database name")
    return name


def _run_sql(conn, statement) -> None:
    with conn.cursor() as cur:
        cur.execute(statement)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reindex the project PostgreSQL database and refresh its collation version."
    )
    parser.add_argument("--connect-timeout", type=int, default=5)
    parser.add_argument("--skip-vacuum", action="store_true")
    args = parser.parse_args()

    db_name = _current_database(args.connect_timeout)
    print(f"Using database: {db_name}")

    print("Running REINDEX DATABASE ...")
    conn = _connect(dbname=db_name, connect_timeout=args.connect_timeout)
    try:
        _run_sql(conn, sql.SQL("REINDEX DATABASE {}").format(sql.Identifier(db_name)))
    finally:
        _close(conn)

    print("Refreshing recorded collation version ...")
    conn = _connect(dbname="postgres", connect_timeout=args.connect_timeout)
    try:
        _run_sql(
            conn,
            sql.SQL("ALTER DATABASE {} REFRESH COLLATION VERSION").format(sql.Identifier(db_name)),
        )
    finally:
        _close(conn)

    if not args.skip_vacuum:
        print("Running VACUUM (ANALYZE) ...")
        conn = _connect(dbname=db_name, connect_timeout=args.connect_timeout)
        try:
            _run_sql(conn, "VACUUM (ANALYZE)")
        finally:
            _close(conn)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
