# Architecture Guide

This repository is a partially modularized Telegram bot codebase. The project is no
longer a small single-file bot, but it is also not fully split into isolated packages
yet. The practical model is:

- `bot.py` as the composition root and integration hub
- feature modules for bounded areas of behavior
- PostgreSQL as the source of truth
- Elasticsearch as an optional search accelerator
- a local dashboard and systemd-based deployment path

## High-Level Components

### Telegram Runtime

- `bot.py`
  Loads configuration, initializes PostgreSQL/Elasticsearch/runtime helpers,
  injects dependencies into feature modules, starts the Telegram application,
  and runs background workers.
- `handler_registry.py`
  Central place where command, message, callback, inline-query, and error
  handlers are registered.
- `command_sync.py`
  Synchronizes Telegram command menus per language and chat scope.

### User-Facing Feature Modules

- `search_flow.py`
  Search entrypoint for text messages, result formatting, result selection,
  audiobook browsing/playback callbacks, and some search caching helpers.
- `upload_flow.py`
  `/upload` flow, upload validation, duplicate checks, indexing hooks, storage
  channel refresh, and local backup download workers.
- `engagement_handlers.py`
  Favorites, reactions, and book action callbacks.
- `user_interactions.py`
  Book request lifecycle and user-facing request status interactions.
- `tts_tools.py`, `pdf_maker.py`, `pdf_editor.py`, `audio_converter.py`,
  `sticker_tools.py`
  Tool-style feature modules exposed from the "Other Functions" menu.

### Admin and Operations

- `admin_runtime.py`
  Owner/admin control panel, task inspection, duplicate cleanup controls, and
  maintenance helpers.
- `dashboard_server.py`
  Local admin dashboard backend serving data from PostgreSQL/Elasticsearch.
- `dashboard_ui/`
  Static frontend for the local dashboard.

### Data and Persistence

- `db.py`
  PostgreSQL pool, runtime schema bootstrap, and a large set of query helpers.
- `alembic/`
  Managed schema migrations for changes that should be versioned explicitly.

## Runtime Flow

### 1. Startup

At startup, `bot.py` does the following:

1. Loads `.env` and process environment via `config.py`
2. Validates required runtime config
3. Initializes DB pool and runtime schema bootstrap
4. Checks Elasticsearch availability if configured
5. Builds dependency dictionaries for feature modules
6. Calls `configure(...)` on modules that rely on runtime injection
7. Registers handlers through `handler_registry.register_handlers(...)`
8. Starts background workers and polling/webhook-related runtime tasks

This pattern keeps the feature modules relatively decoupled without introducing a
full dependency-injection framework.

### 2. Search and Delivery

Private-chat search path:

1. User sends text
2. First-time users without explicit language selection are redirected back to the
   language picker instead of running search immediately
3. `search_flow.search_books(...)` normalizes the query and checks cache
4. Search uses Elasticsearch first when available, with DB/local fallbacks
5. Results are ranked and trimmed to the top 10 matches
6. Numeric inline buttons (`1–10`) keep selection logic simple and backward-compatible
7. Selected books are sent through shared helpers in `bot.py`

Group-chat search path is separate:

- users must first start the bot in private chat
- group replies use a dedicated `group_language`
- the bot prompts for group language before group search becomes active

### 3. Upload Flow

`upload_flow.py` handles both direct owner/admin uploads and upload-request logic:

1. Validate permissions and upload mode
2. Validate file extension and title/adult-content filters
3. Insert/update canonical DB record
4. Refresh Telegram storage-channel `file_id` when needed
5. Queue local backup download work
6. Index the book in Elasticsearch if configured

The upload system is tightly linked to local backup storage and storage-channel
reupload behavior, so it is one of the more operationally sensitive parts of the repo.

### 4. Background Jobs

The project uses two async/background patterns:

- in-process asyncio tasks for lightweight runtime work
- DB-backed jobs in `background_jobs` for heavier or restart-safe workflows

Examples of DB-backed or restart-safe work in the repo include:

- sticker/background media processing
- some long-running content preparation tasks
- local backup download queues for books/audiobooks

## Data Model

Primary store: PostgreSQL

Important table groups:

- catalog: `books`, `audio_books`, `audio_book_parts`
- user state: `users`, `user_favorites`, `user_recents`
- engagement: `book_reactions`, `user_favorite_awards`, `user_reaction_awards`
- requests: `book_requests`, `upload_requests`
- progress: `user_audiobook_progress`, `user_audiobook_part_history`
- ops/runtime: `background_jobs`, `bot_settings`, `schema_migrations`

Search index:

- Elasticsearch index: `books`

The database remains the source of truth. Elasticsearch is an optimization layer,
not the canonical data store.

## Configuration Model

Config is loaded from:

- project `.env`
- process environment
- optional systemd `EnvironmentFile`

Notable behavior:

- `.env` is loaded using an absolute path so systemd working-directory changes do
  not break config resolution
- `.env` does not override existing process environment by default
- `.env.example` is the public template and intentionally contains placeholders only

## Deployment Layout

The repository ships public-safe templates in `systemd/`:

- `pdf_audio_kitoblar_bot.service` — local Telegram Bot API server
- `pdf_audio_kitoblar_bot-bot.service` — main bot process
- `pdf_audio_kitoblar_bot-dashboard.service` — local dashboard
- `pdf_audio_kitoblar_bot-stack.target` — optional grouping target
- `pdf_audio_kitoblar_bot-hotkey.sudoers` — example sudoers snippet

These files are templates. They must be customized before installation.

## Testing and Diagnostics

This repo relies mostly on smoke/regression scripts, not a large formal test suite.

Useful files:

- `test_bot_start.py` — broad import/startup smoke check
- `test_imports.py` — search/import smoke check
- `test_upload_mode.py`, `test_upload_system.py` — upload-related checks
- `check_*.py`, `debug_*.py`, `*_diagnosis.py` — focused diagnostics

When changing runtime wiring, handler registration, or imports, `test_bot_start.py`
is usually the first check worth running.

## Practical Contributor Notes

- Start with `README.md` for setup and commands.
- Read `handler_registry.py` to understand the Telegram surface area quickly.
- Use `bot.py` as the composition map, not as the first place to put new business logic.
- Prefer adding helpers to the relevant feature module instead of growing `bot.py`.
- Treat `db.py` changes carefully: it mixes runtime bootstrap, schema compatibility,
  and data-access helpers in one file.

## Known Tradeoffs

- `bot.py` is still large and owns a lot of shared behavior.
- `db.py` mixes migrations/bootstrap/query logic more than ideal.
- The codebase has many operational helper scripts intended for live maintenance.
- Some newer features are cleanly extracted, while older flows still depend on
  shared globals injected at runtime.

That means the repository is workable and production-focused, but contributors
should favor incremental refactors over sweeping rewrites.
