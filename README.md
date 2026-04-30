# 📚 pdf_audio_kitoblar_bot

Telegram bot backend for a digital library focused on:

- PDF books
- audiobooks
- PDF tools
- text-to-voice
- media utilities
- admin operations
- local dashboard

The codebase is a production-style Telegram bot with a large integration entrypoint
(`bot.py`) and extracted feature modules for search, uploads, engagement, admin
runtime, and dashboard operations.

## Documentation Map

- `README.md` — product overview, setup, commands, and deployment notes
- `docs/ARCHITECTURE.md` — repository layout, runtime flow, and module guide
- `docs/worldlibrarybot_menu_map.svg` — visual menu map
- `UI_UX_RECOMMENDATIONS.md` — backlog-style UX analysis and improvement ideas

## Screenshots

### Start Menu
<p align="center">
  <img src="https://github.com/user-attachments/assets/048ccb76-45c4-4e8f-823b-024f25d068cb" width="300"/>
  <img src="https://github.com/user-attachments/assets/d87ab4e4-8ef7-474e-8645-040e48a41373" width="300"/>
  <img src="https://github.com/user-attachments/assets/161dd8af-b52d-4530-9eeb-85e286b115d8" width="300"/>
</p>

### Book Delivery
<p align="center">
  <img src="https://github.com/user-attachments/assets/e18974b1-ebbd-44bb-8089-3224b7703323" width="300"/>
</p>

### Other Features
<p align="center">
  <img src="https://github.com/user-attachments/assets/0400cba7-6df3-435e-98b5-3a9c860cb543" width="300"/>
  <img src="https://github.com/user-attachments/assets/c9051d29-6836-4c0a-9f94-fa59959b5c7d" width="300"/>
</p>

### Admin Panel
<p align="center">
  <img src="https://github.com/user-attachments/assets/14e3be35-c68f-4082-954a-6b0baec91f85" width="300"/>
</p>

### Owner/Admin Media Control
<p align="center">
  <img src="https://github.com/user-attachments/assets/379b4afd-98d3-4b2a-9a81-90c53d2e5dad" width="300"/>
</p>

## Quick Start

Prerequisites:

- Python 3.12 recommended
- PostgreSQL required
- `ffmpeg` required for audio / sticker / media utilities
- Elasticsearch optional
- Local Telegram Bot API optional

Install and run locally:

```bash
python3.12 -m venv venv312
source venv312/bin/activate
pip install -r requirements.txt
cp .env.example .env
python bot.py
```

Then fill in your real bot token, owner ID, database credentials, and any optional
service endpoints you use.

## Current Feature Scope

Active user-facing features:

- Book search and delivery in private chat and inline mode
- Ranked search results limited to the top 10 best matches per query
- First-run language selection before private-chat search starts
- Audiobook listening per book with part navigation and listened-state tracking
- Book requests when search misses (`/request`, `/requests`, and no-results actions)
- Upload access requests for non-owner `/upload` usage
- Favorites, reactions, top books, top users, and user profile stats
- PDF Maker
- Text to Voice
- Audio Editor
- PDF Editor
- Sticker Tools with DB-backed background jobs
- Contact Admin card
- Local dashboard and owner/admin control tools

Removed from the product:

- Movie search
- Movie upload
- Movie analytics
- Legacy AI chat / translator / grammar / email / quiz / music tools
- Separate AI Tools menu

## Architecture

- Telegram bot runtime: `python-telegram-bot`
- Primary storage: PostgreSQL
- Search index: Elasticsearch (`books` only)
- Background jobs: PostgreSQL queue (`background_jobs`)
- Media processing: `ffmpeg`
- TTS backends: `edge-tts`, optional `espeak-ng`
- Downloader: `yt-dlp`
- Dashboard: local web UI + Python backend

## Repository Layout

Main runtime and feature modules:

- `bot.py` — composition root, startup, shared helpers, send/delivery logic, and
  background job worker loop
- `handler_registry.py` — centralized `python-telegram-bot` handler registration
- `search_flow.py` — text search, result rendering, selection flow, and audiobook UI
- `upload_flow.py` — `/upload`, storage-channel refresh, and local backup workers
- `engagement_handlers.py` — favorites, reactions, and per-book action callbacks
- `user_interactions.py` — request lifecycle and related callback handling
- `admin_runtime.py` — owner/admin control panel and maintenance flows
- `db.py` — PostgreSQL pool, runtime schema bootstrap, and query helpers
- `dashboard_server.py` + `dashboard_ui/` — local admin dashboard backend/frontend
- `command_sync.py` — per-language Telegram command synchronization
- `language.py`, `menus.py`, `menu_ui.py` — multilingual copy and menu definitions

Operational and support files:

- `alembic/` — schema migrations for managed DB changes
- `systemd/` — public-safe deployment templates
- `test_*.py` — smoke/regression scripts
- `check_*.py`, `debug_*.py`, `*_diagnosis.py` — operational diagnostics and repair helpers

## Main Menus

Main menu:

- `🔎 Search Books`
- `⭐ Favorites`
- `👤 My Profile`
- `🔥 Top Books`
- `🛠️ Other Functions`
- `🛠 Admin Control` (owner only)

Other Functions:

- `🎙️ Text to Voice`
- `🤖 AI PDF Maker`
- `🧰 PDF Editor`
- `🎛️ Audio Editor`
- `🧩 Sticker Tools`
- `🏆 Top Users`
- `📞 Contact Admin`
- `❓ Help`

## Commands

Public Telegram command menu (synced for private/group chats):

- `/start`
- `/random`
- `/upload`
- `/language`
- `/help`

Implemented but not shown in the default public command menu:

- `/myprofile`
- `/favorite`
- `/top`
- `/top_users`
- `/mystats`
- `/request`
- `/requests`
- `/pdf_maker`
- `/pdf_editor`
- `/text_to_voice`
- `/sticker_tools`

Owner/admin commands:

- `/admin`
- `/upload`
- `/smoke`
- `/db_dupes`
- `/es_dupes`
- `/dupes_status`
- `/cancel_task`
- `/user`
- `/pause_bot`
- `/resume_bot`
- `/broadcast`
- `/audit`
- `/prune`
- `/missing`
- `/chatid`

## Data Model

Core PostgreSQL tables:

- `users`
- `books`
- `audio_books`
- `audio_book_parts`
- `audio_book_local_download_jobs`
- `upload_receipts`
- `book_requests`
- `upload_requests`
- `book_reactions`
- `user_favorites`
- `user_favorite_awards`
- `user_reaction_awards`
- `user_recents`
- `user_audiobook_progress`
- `user_audiobook_part_history`
- `book_summaries`
- `background_jobs`
- `analytics_daily`
- `analytics_daily_users`
- `analytics_counters`

Elasticsearch indexes:

- `books`

## Environment

Required:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_OWNER_ID`
- `DB_NAME`
- `DB_USER`
- `DB_PASS`
- `DB_HOST`
- `DB_PORT`

Common optional:

- `REQUEST_CHAT_ID`
- `BOOK_STORAGE_CHANNEL_ID`
- `AUDIO_UPLOAD_CHANNEL_ID`
- `AUDIO_UPLOAD_CHANNEL_IDS`
- `VIDEO_UPLOAD_CHANNEL_ID`
- `VIDEO_UPLOAD_CHANNEL_IDS`
- `ES_URL`
- `ES_USER`
- `ES_PASS`
- `ES_CA_CERT`
- `BOOK_THUMBNAIL_PATH`
- `TELEGRAM_BOT_API_BASE_URL`
- `TELEGRAM_BOT_API_BASE_FILE_URL`
- `TELEGRAM_BOT_API_LOCAL_MODE`

## Startup

Run the bot locally:

```bash
source venv312/bin/activate
python bot.py
```

Systemd deployment in this repo includes:

- `systemd/pdf_audio_kitoblar_bot.service`
- `systemd/pdf_audio_kitoblar_bot-bot.service`
- `systemd/pdf_audio_kitoblar_bot-dashboard.service`
- `systemd/pdf_audio_kitoblar_bot-stack.target`
- `systemd/pdf_audio_kitoblar_bot-hotkey.sudoers` (example only)

The `systemd/` files are deployment templates. Before installing them, replace the
example user, group, project directory, virtualenv path, and sudoers account values
to match your own server.

## Verification

Useful smoke checks after edits:

```bash
python test_bot_start.py
python test_imports.py
python test_upload_mode.py
python test_upload_system.py
```

Notes:

- `test_bot_start.py` is the fastest broad import/startup smoke check.
- `test_imports.py` writes a simple import report to `import_test_results.txt`.
- These are smoke-style checks, not a full pytest suite.

## Notes

- This repository does not track `.env`, live credentials, runtime logs, or local media storage.
- Dashboard fallback values are demo/sample data for local UI development only.
- Books are the only indexed search catalog now.
- Public command menus are synced dynamically per chat/language.
- Sticker conversion and some heavy tasks run through the DB-backed background job queue.
- Legacy removed feature tables and counters are cleaned by schema migration.
- Group chat usage has a separate onboarding path: users must start the bot once in
  private chat, then choose a group reply language.
