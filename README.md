# 📚 PDF & Audiobook Telegram Bot

A powerful Telegram bot for managing and delivering **PDF books and audiobooks** with advanced features like search, TTS, media tools, and admin control panel.

---

## 🚀 Features

### 📖 Core Library Features
- Search and download PDF books
- Audiobook streaming with progress tracking
- Inline search support
- Smart request system when content is missing

### 🎧 Media & Tools
- Text-to-Speech (TTS)
- PDF Maker & PDF Editor
- Audio Editor
- Sticker Tools (with background processing)

### 👤 User Features
- Favorites system
- User profile & statistics
- Top books & top users
- Reactions & engagement system

### 🛠 Admin Features
- Admin control panel inside Telegram
- Broadcast system
- User management tools
- Duplicate detection (DB + Elasticsearch)
- Background job monitoring

---

## 📸 Screenshots

### 🔹 Start Menu
<p align="center">
  <img src="https://github.com/user-attachments/assets/048ccb76-45c4-4e8f-823b-024f25d068cb" width="300"/>
  <img src="https://github.com/user-attachments/assets/d87ab4e4-8ef7-474e-8645-040e48a41373" width="300"/>
  <img src="https://github.com/user-attachments/assets/161dd8af-b52d-4530-9eeb-85e286b115d8" width="300"/>
</p>

### 🔹 Book Delivery
<p align="center">
  <img src="https://github.com/user-attachments/assets/e18974b1-ebbd-44bb-8089-3224b7703323" width="300"/>
</p>

### 🔹 Other Features
<p align="center">
  <img src="https://github.com/user-attachments/assets/0400cba7-6df3-435e-98b5-3a9c860cb543" width="300"/>
  <img src="https://github.com/user-attachments/assets/c9051d29-6836-4c0a-9f94-fa59959b5c7d" width="300"/>
</p>

### 🔹 Admin Panel
<p align="center">
  <img src="https://github.com/user-attachments/assets/14e3be35-c68f-4082-954a-6b0baec91f85" width="300"/>
</p>


## Owner-admin only media control 
<p align="center">
  <img src="https://github.com/user-attachments/assets/379b4afd-98d3-4b2a-9a81-90c53d2e5dad" width="300"/>
</p>

---

## 🏗 Architecture

- **Backend:** Python (`python-telegram-bot`)
- **Database:** PostgreSQL
- **Search Engine:** Elasticsearch
- **Media Processing:** ffmpeg
- **TTS:** edge-tts, espeak-ng
- **Queue System:** PostgreSQL-based background jobs
- **Dashboard:** Local web UI

---

## 📂 Key Functionalities

- Smart book search system (Elasticsearch-powered)
- Audiobook progress tracking per user
- Request system for unavailable content
- Background job queue for heavy tasks
- Admin analytics & monitoring

---

## ⚙️ Installation

```bash
git clone https://github.com/your-username/pdf-audio-kitoblar-bot
cd pdf-audio-kitoblar-bot
cp .env.example .env
