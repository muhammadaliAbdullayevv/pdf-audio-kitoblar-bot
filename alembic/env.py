from __future__ import annotations

from logging.config import fileConfig
import os
from urllib.parse import quote_plus

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    try:
        fileConfig(config.config_file_name)
    except Exception:
        # Logging config is optional for this project.
        pass

target_metadata = None


def _database_url_from_env() -> str:
    direct = str(os.getenv("DATABASE_URL", "") or "").strip()
    if direct:
        return direct

    db_name = str(os.getenv("DB_NAME", "") or "").strip()
    db_user = str(os.getenv("DB_USER", "") or "").strip()
    db_pass = str(os.getenv("DB_PASS", "") or "").strip()
    db_host = str(os.getenv("DB_HOST", "") or "").strip()
    db_port = str(os.getenv("DB_PORT", "5432") or "5432").strip()
    if not (db_name and db_user and db_host):
        return ""
    return (
        "postgresql+psycopg2://"
        f"{quote_plus(db_user)}:{quote_plus(db_pass)}@{db_host}:{db_port}/{quote_plus(db_name)}"
    )


def _configure_database_url() -> None:
    url = _database_url_from_env()
    if url:
        config.set_main_option("sqlalchemy.url", url)


def run_migrations_offline() -> None:
    _configure_database_url()
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError("Alembic requires DATABASE_URL or DB_* env vars for offline migrations.")

    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    _configure_database_url()
    section = config.get_section(config.config_ini_section) or {}
    if not section.get("sqlalchemy.url"):
        raise RuntimeError("Alembic requires DATABASE_URL or DB_* env vars for online migrations.")

    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
