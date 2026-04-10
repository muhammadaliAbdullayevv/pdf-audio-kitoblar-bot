# SmartAIToolsBot

Telegram bot backend for a digital library focused on:

- PDF books
- audiobooks
- PDF tools
- text-to-voice
- media utilities
- admin operations
- local dashboard

## Current Feature Scope

Active user-facing features:

- Book search and delivery
- Audiobook listening per book
- Book requests when search misses
- Favorites, reactions, top books, top users
- PDF Maker
- AI PDF Translator
- Text to Voice
- Audio Editor
- PDF Editor
- Sticker Tools
- YouTube / Instagram downloader

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
- Media processing: `ffmpeg`
- TTS backends: `edge-tts`, optional `espeak-ng`
- Downloader: `yt-dlp`
- Dashboard: local web UI + Python backend

## Main Menus

Main menu:

- `🔎 Search Books`
- `🔥 Top Books`
- `🎙️ Text to Voice`
- `⬇️ Insta Youtub`
- `🛠️ Other Functions`

Other Functions:

- `🤖 AI PDF Maker`
- `🌐📄 AI PDF Translator`
- `🎛️ Audio Editor`
- `🧰 PDF Editor`
- `🧩 Sticker Tools`
- `🏆 Top Users`
- `📞 Contact Admin`
- `❓ Help`

## Commands

Visible private-chat user commands:

- `/start`
- `/language`
- `/myprofile`
- `/favorite`
- `/request`
- `/requests`

Implemented but hidden from the default command menu:

- `/help`
- `/pdf_maker`
- `/pdf_editor`
- `/text_to_voice`
- `/sticker_tools`
- `/top`
- `/top_users`
- `/mystats`

Owner/admin commands:

- `/upload`
- `/admin`
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

Group commands:

- `/start`
- `/language`
- `/random`

## Data Model

Core PostgreSQL tables:

- `users`
- `books`
- `audio_books`
- `audio_book_parts`
- `upload_receipts`
- `book_requests`
- `upload_requests`
- `book_reactions`
- `user_favorites`
- `user_favorite_awards`
- `user_reaction_awards`
- `user_recents`
- `book_summaries`
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

- local Telegram Bot API service
- main bot service
- dashboard service
- stack target

## Notes

- Books are the only indexed search catalog now.
- Legacy removed feature tables and counters are cleaned by schema migration.
- Legacy removed feature tables and counters are cleaned by schema migration.
