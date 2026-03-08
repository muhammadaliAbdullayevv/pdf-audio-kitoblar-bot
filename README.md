# SmartAIToolsBot

Telegram bot for searching and delivering books/audiobooks, with multilingual UI, dynamic menus, AI tools (local Ollama/NLLB), TTS, PDF Maker, admin control, and a beta video downloader.

## What This Bot Does

- Search books (DB + optional Elasticsearch) with inline pagination and download buttons
- Search movies (DB + optional Elasticsearch) with book-like result flow
- Deliver books from cached Telegram `file_id` or local files
- Store audiobook parts in a dedicated Telegram channel (optional) and keep `channel_id`/`channel_message_id` in DB
- Dynamic menu-first UX (Uzbek / English / Russian)
- AI tools menu:
  - AI Chat (local Ollama)
  - AI Translator (NLLB-200 local backend with optional Ollama fallback)
  - AI Grammar Fix
  - AI Email Writer
- Text to Voice (Edge TTS + optional AI text polishing)
- PDF Maker (step-by-step wizard, text-only PDF output, optional AI font-size selection)
- Audio Editor, PDF Editor, and Sticker Tools in Other Functions
- Favorites, reactions, top books/users, profile, requests
- Admin Control (reply-keyboard panel, no slash-command dependency for routine admin tasks)
- Beta Video Downloader (YouTube/Instagram public links)
- Name Meanings menu scaffold (coming soon)
- Bot profile texts (description/about) are synced per language (`en`, `uz`, `ru`) on startup

## Current UX (Important)

This bot is **menu-first**.

- Most user features are accessed via dynamic reply-keyboard menus
- Slash commands are kept minimal / fallback
- Public users keep personal commands in command menu:
  - `/start`, `/language`
  - `/myprofile`, `/favorite`
  - `/request`, `/requests`
  - `/my_quiz`
- Upload commands are **not** in public command menu; they are scoped to admin/owner command menus.

### Group Chat Behavior

- In groups, command list is limited to:
  - `/start`
  - `/language`
- Group users can search books by sending a **reply message** with a book name (reply-to-message search)
- The bot avoids noisy "tap Search Books first" prompts in groups

### Bot Profile Text Localization

- On startup, bot calls Telegram APIs to set:
  - `setMyDescription`
  - `setMyShortDescription`
- Current language payloads are defined in `BOT_PROFILE_TEXTS` inside `bot.py`
- Default + language-specific entries are synced for `en`, `uz`, `ru`

## Main Menus

### Main Menu
- `🔎 Search Books`
- `🎬 Search Movies`
- `🤖 AI Tools`
- `🎙️ Text to Voice`
- `⬇️ Insta Youtub` (beta)
- `🌙 Ramadan Duas`
- `🛠️ Other Functions`
- `🛠 Admin Control` (admin/owner only; shown in the first row)

### AI Tools Menu
- `💬 Chat with AI`
- `🌐 AI Translator`
- `✍️ AI Grammar Fix`
- `📧 AI Email Writer`
- `📝 AI Quiz Generator`
- `🎵 AI Music Generator`
- `🤖 AI PDF Maker`
- `🌐📄 AI PDF Translator`
- `🖼️ AI Image Generator` (currently coming soon)

### Other Functions Menu
- `🔥 Top Books`
- `🏆 Top Users`
- `🎛️ Audio Editor`
- `🧰 PDF Editor`
- `🧩 Sticker Tools`
- `🪪 Name Meanings` (coming soon)
- `📞 Contact Admin`
- `❓ Help`

Notes:
- `My Profile`, `Favorites`, and `Request Book` are available from slash commands.
- Upload actions are command-based (`/upload`, `/movie_upload`) for admin/allowed users.

## AI Tools (Current Behavior)

### AI Chat
- Uses local Ollama model
- Replies in the language of the user message (content language), not just UI language
- Has built-in smart replies for bot identity/owner questions
- Friendly content-based style with cleanup (no raw encoded links garbage)
- Active mode keyboard shows status + `Change AI Tool` / `Exit AI Tool`

### AI Translator
- Primary backend: `NLLB-200` (local)
- Optional fallback: Ollama
- Recommended input format (best accuracy):
  - `uz>en: Assalomu alaykum`
- Quick format (target only):
  - `en: Assalomu alaykum`
- Active mode keyboard shows status + `Change AI Tool` / `Exit AI Tool`

### AI Grammar Fix
- Fixes grammar/spelling/punctuation
- Keeps the same language and meaning
- Active mode keyboard shows status + `Change AI Tool` / `Exit AI Tool`

### AI Email Writer
- Drafts polite emails/letters in user language
- Active mode keyboard shows status + `Change AI Tool` / `Exit AI Tool`

## Video Downloader (Beta)

### Supported sources
- YouTube (public links)
- Instagram (public links)

### Current beta limits
- **Max file size:** `15 MB`
- **Per-user limit:** `3 successful downloads total` (test mode)
- After quota is reached, bot replies with a test-mode availability message

### Flow
1. User opens `⬇️ Insta Youtub` from the main menu
2. Sends a public link
3. Bot checks metadata and sends preview card (thumbnail + title/channel + qualities/sizes)
4. Inline quality buttons appear (dynamic, based on available formats)
5. Bot downloads in background and updates progress
6. Bot sends video/audio to Telegram

### Notes
- Large links are rejected early with a friendly "send a smaller video" message
- `MP3` button sends audio, video quality buttons send video
- `Trim` is placeholder (coming soon)
- `Preview` resends the preview card

## Upload Flow (Current)

- `/upload` enables **book** upload mode for allowed users/admins
- `/movie_upload` enables **movie** upload mode for allowed users/admins
- For each file, user-facing status is sent once:
  - `Saved ...`
  - `Duplicate ...`
- No extra `Indexed ...` message is sent to chat
- DB save happens first, Elasticsearch indexing runs in background
- Indexing state is tracked in DB (`upload_receipts.status`, `books.indexed`)
- Set `UPLOAD_NO_STATUS_EDITS=1` to avoid "loading" edits and send reply-only status messages
- ES bulk indexing queue flushes when either threshold is hit:
  - `UPLOAD_ES_BULK_SIZE` (default `100`)
  - `UPLOAD_ES_BULK_IDLE_TIMEOUT_SEC` (default `10`)

## Text to Voice (TTS)

- Step-by-step UI via inline buttons (language/voice/gender/tone/speed/output)
- Cleaner UI (reduced instruction clutter)
- `AI Voice Booster` toggle (renamed from generic AI toggle)
- Improved language auto-detection behavior (less false Uzbek detection on English text)
- Uses local `edge_tts` + `ffmpeg`

## PDF Maker

- Step-by-step wizard:
  1. PDF name
  2. Style (`AI Style` / `Simple`)
  3. Paper (`A4` / `Letter`)
  4. Orientation (`Portrait` / `Landscape`)
  5. Continue
  6. Send text
- Output is **text-only PDF** (no title/date/header/footer inside the PDF body)
- `AI Style` can auto-select a more readable body font size (Ollama + heuristic fallback)

## Admin Control

Admin UX is reply-keyboard based (not inline panel), and available only to admin/owner users.

Examples of actions inside Admin Control:
- Search users (name / username / full or partial user ID)
- Upload books/movies via `/upload` and `/movie_upload` (admin-scoped command menu)
- Audit report
- Pause / Resume bot
- Prune users
- Missing files preview / confirm cleanup
- Duplicate previews / status (DB + ES)
- Cancel background tasks
- Local bulk upload tools (`all`, `missing`, `unique`, `large`, `status`)

### Owner command visibility
Owner/admin command menu is scoped and includes upload commands (`/upload`, `/movie_upload`), while public users do not see these upload commands.

## Modular Code Layout (Current)

The bot was refactored out of a single large file into feature modules. `bot.py` now mainly wires modules together and provides shared utilities.

- `bot.py` - app bootstrap, handler registration, shared utilities, module bridges
- `config.py` - environment config loading
- `db.py` - PostgreSQL schema + queries
- `language.py` - localized message dictionary
- `menu_ui.py` - menu text parsing/help/admin labels
- `menus.py` - dynamic menu keyboard and menu text builders
- `admin_tools.py` - admin menu routing + admin prompt handlers
- `admin_runtime.py` - admin runtime commands/callbacks (audit, dupes, tasks, user search, local uploads)
- `search_flow.py` - book search flow + paging + selection callbacks
- `tts_tools.py` - Text-to-Voice feature
- `pdf_maker.py` - PDF Maker feature
- `ai_tools.py` - AI Chat / Translator / Grammar / Email / AI image placeholders
- `video_downloader.py` - video downloader beta feature
- `engagement_handlers.py` - favorites/reactions/top/summary/delete callbacks
- `user_interactions.py` - requests/profile/help/favorites user commands and callbacks
- `upload_flow.py` - upload commands and file/photo upload processing

## Requirements

### Core
- Python `3.12+` (project currently uses `venv312`)
- PostgreSQL
- Telegram bot token

### Optional (recommended)
- Elasticsearch 8.x (faster search)
- Ollama (AI tools / PDF/TTS enhancements)

### Python packages
Core packages are in `requirements.txt`:
- `python-telegram-bot==20.7`
- `psycopg2-binary`
- `elasticsearch==8.15.0`
- `rapidfuzz`
- `python-dotenv`
- plus others in the file

Additional optional dependencies used by features (install as needed):
- `transliterate` (if your code path uses transliteration package imports)
- `reportlab` (PDF rendering)
- `pypdf` (PDF text extraction)
- `edge-tts` (TTS)
- `torch`, `transformers`, `sentencepiece`, `safetensors` (NLLB translator)

## Quick Start (Manual)

```bash
cd ~/Documents/SmartAIToolsBot
python3 -m venv venv312
source venv312/bin/activate
pip install -r requirements.txt
# Optional feature deps:
# pip install reportlab pypdf edge-tts transliterate torch transformers sentencepiece safetensors
python3 bot.py
```

## Environment Variables (`.env`)

### Required
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_OWNER_ID`
- `DB_NAME`
- `DB_USER`
- `DB_PASS`
- `DB_HOST`
- `DB_PORT`

### Core Optional (bot only)
- `TELEGRAM_ADMIN_ID`
- `REQUEST_CHAT_ID`
- `UPLOAD_CHANNEL_ID`
- `UPLOAD_CHANNEL_IDS` (comma-separated)
- `AUDIO_UPLOAD_CHANNEL_ID` (audiobook media storage channel)
- `VIDEO_UPLOAD_CHANNEL_ID` (reserved for video storage flow)

### Upload Flow Tuning
- `UPLOAD_NO_STATUS_EDITS` (`1` = no "loading" edit; reply-only final status)
- `UPLOAD_ES_BULK_SIZE` (ES bulk queue flush size, default `100`)
- `UPLOAD_ES_BULK_IDLE_TIMEOUT_SEC` (ES bulk queue max wait, default `10`)
- `UPLOAD_SKIP_NAME_DUP_CHECK` (`1` disables extra duplicate check by normalized name)
- `UPLOAD_SKIP_REQUEST_NOTIFY` (`1` disables request-match notifications)
- `UPLOAD_FANOUT_RETRY_MAX`
- `UPLOAD_FANOUT_SEND_DELAY_SEC`
- `UPLOAD_FANOUT_RETRY_JITTER_SEC`

### Self-Hosted Telegram Bot API (Optional)
- `TELEGRAM_BOT_API_BASE_URL` (example: `http://127.0.0.1:8081`)
- `TELEGRAM_BOT_API_BASE_FILE_URL` (optional; if empty, code derives `/file/bot` from base URL)
- `TELEGRAM_BOT_API_LOCAL_MODE` (`1` for local mode)

### Search / Elasticsearch
- `ES_URL`
- `ES_USER`
- `ES_PASS`
- `ES_CA_CERT`

### Coins / Rankings
- `COIN_SEARCH`
- `COIN_DOWNLOAD`
- `COIN_REACTION`
- `COIN_FAVORITE`
- `COIN_REFERRAL`
- `TOP_USERS_LIMIT`

### Ollama / AI Tools
- `OLLAMA_URL`
- `AI_CHAT_OLLAMA_MODEL`
- `AI_CHAT_OLLAMA_TIMEOUT`
- `AI_TOOLS_OLLAMA_TIMEOUT`
- `TTS_OLLAMA_MODEL`
- `TTS_OLLAMA_TIMEOUT`
- `PDF_MAKER_OLLAMA_MODEL`
- `PDF_MAKER_OLLAMA_TIMEOUT`

### AI Translator (NLLB)
- `AI_TRANSLATOR_BACKEND` (`nllb` or `ollama`)
- `AI_TRANSLATOR_NLLB_MODEL` (example: `facebook/nllb-200-distilled-600M`)
- `AI_TRANSLATOR_NLLB_DEVICE` (`cpu` recommended on low-VRAM systems)
- `AI_TRANSLATOR_NLLB_LOCAL_ONLY` (`0/1`)
- `AI_TRANSLATOR_NLLB_MAX_INPUT_TOKENS`
- `AI_TRANSLATOR_NLLB_MAX_NEW_TOKENS`
- `AI_TRANSLATOR_FALLBACK_OLLAMA` (`0/1`)

### AI Image (future/local backend integration placeholder)
- `AI_IMAGE_SD_API_URL`
- `AI_IMAGE_SD_CHECKPOINT`
- `AI_IMAGE_WIDTH`
- `AI_IMAGE_HEIGHT`
- `AI_IMAGE_STEPS`
- `AI_IMAGE_CFG`
- `AI_IMAGE_SAMPLER`
- `AI_IMAGE_COUNT`
- `AI_IMAGE_TIMEOUT`

### Video Downloader (beta)
- `VIDEO_DL_MAX_MB`
  - Current code hard-caps this to `15 MB` during test mode

## NLLB Translator Setup (Local)

Recommended for this bot:
- `AI_TRANSLATOR_BACKEND=nllb`
- `AI_TRANSLATOR_NLLB_MODEL=facebook/nllb-200-distilled-600M`
- `AI_TRANSLATOR_NLLB_DEVICE=cpu`

Example install (inside `venv312`):
```bash
pip install torch transformers sentencepiece safetensors
```

## systemd (Recommended Production Run)

Recommended production setup uses the stack target:
- `SmartAIToolsBot.service` (local Telegram Bot API)
- `SmartAIToolsBot-bot.service` (main bot app)
- `SmartAIToolsBot-stack.target` (starts/stops both together)

Common commands:
```bash
sudo systemctl enable --now SmartAIToolsBot-stack.target
sudo systemctl restart SmartAIToolsBot-stack.target
sudo systemctl status SmartAIToolsBot-stack.target SmartAIToolsBot.service SmartAIToolsBot-bot.service --no-pager
sudo journalctl -fu SmartAIToolsBot.service -fu SmartAIToolsBot-bot.service -l
```

### Important: avoid duplicate bot instances
If two instances run with the same token, Telegram long polling fails with:
- `Conflict: terminated by other getUpdates request`

Only one of these should run:
- manual `python bot.py`
- `systemctl SmartAIToolsBot-bot.service`
- `systemctl SmartAIToolsBot-stack.target`

### Wi-Fi driven start/stop (NetworkManager dispatcher)

Repository script:
- `systemd/SmartAIToolsBot-nm-dispatcher.sh`

Install hook (as root):
```bash
sudo install -m 0755 systemd/SmartAIToolsBot-nm-dispatcher.sh /etc/NetworkManager/dispatcher.d/90-smartaitoolsbot
sudo systemctl enable --now NetworkManager-dispatcher.service
```

Behavior:
- Wi-Fi disconnected: stop bot + local Bot API once
- Wi-Fi reconnected after disconnect: restart bot + local Bot API once
- Duplicate events in same connectivity state: no action

Useful checks:
```bash
sudo journalctl -t smartaitoolsbot-dispatcher -f -l
sudo systemctl status SmartAIToolsBot-stack.target SmartAIToolsBot.service SmartAIToolsBot-bot.service --no-pager
```

## VS Code Setup (Recommended)

Set the project interpreter to `venv312` so editor diagnostics match runtime:

`.vscode/settings.json`
```json
{
  "python.defaultInterpreterPath": "/home/muhammadaliabdullayev/Documents/SmartAIToolsBot/venv312/bin/python3"
}
```

## Search and Delivery Notes

- Queries are normalized and deduplicated by stable UUID
- Elasticsearch is used when available; DB/local fallback exists
- Inline result pagination and numbered selection buttons are used
- Book delivery prefers Telegram `file_id`, then falls back to local file path

## Data / Storage

- PostgreSQL is the source of truth
- Elasticsearch is an optional fast-search index
- `downloads/` stores local book files
- Telegram `file_id` caching reduces re-uploads
- `upload_receipts` tracks upload pipeline states (`received`, `saved_db`, `indexed`, `duplicate`, `index_failed`, etc.)
- `books` stores optional storage pointers (`storage_chat_id`, `storage_message_id`)
- `movies` stores movie media pointers/metadata and indexing state
- `audio_book_parts` stores optional channel source pointers (`channel_id`, `channel_message_id`)
- `name_meanings` table exists as schema scaffold for future name-meaning feature

## Troubleshooting

### Bot says "Conflict: terminated by other getUpdates request"
You have more than one bot process/service running.

Check:
```bash
ps -ef | grep '[p]ython.*bot.py'
systemctl --user list-units --type=service | grep -Ei 'smartai|bot|telegram'
sudo systemctl status SmartAIToolsBot-bot.service
```

### systemd service fails but manual run works
Your service may be using a different virtual environment.

Check:
```bash
sudo systemctl cat SmartAIToolsBot-bot.service
```
Verify `ExecStart` points to `venv312`.

### `Failed to restart ... service: Unit ... is masked`
The unit is masked and cannot start/restart until unmasked.

Fix:
```bash
sudo systemctl unmask SmartAIToolsBot.service SmartAIToolsBot-bot.service
sudo systemctl daemon-reload
sudo systemctl restart SmartAIToolsBot-stack.target
```

### Upload shows "processing/loading" too long
- Set `UPLOAD_NO_STATUS_EDITS=1` to avoid edit-based loading status
- Confirm DB write + ES worker activity in logs:
```bash
sudo journalctl -u SmartAIToolsBot-bot.service -f -l
```

### Video Downloader works for small files but not big files
Expected in current beta:
- 15 MB max size
- 3 downloads per user (test mode)

### Pylance shows `... is not defined` in extracted modules
Some modules use runtime dependency injection (`configure(globals())`).
These can be editor warnings, not runtime errors.

## Security Notes

- Never commit `.env`
- Restrict admin access to owner/admin IDs only
- Keep DB and Elasticsearch credentials private
- Use local-only (`127.0.0.1`) for internal services when possible

## Project Status

The bot is actively customized and optimized for menu-based UX, multilingual users, and local AI tooling. The codebase is modularized, but continued cleanup/testing is still recommended as features evolve.
