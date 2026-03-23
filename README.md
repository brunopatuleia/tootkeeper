# Mastoferr

A self-hosted Mastodon activity archiver with full-text search, profile updater, and automated toots.

> Built entirely through vibe coding with [Claude Code](https://docs.anthropic.com/en/docs/claude-code). Security reviewed by [Claude Code](https://claude.ai/claude-code) and Gemini Code Assist.

## Features

- Archive toots, notifications, favorites, bookmarks, and media
- Full-text search (SQLite FTS5)
- Profile updater — now-playing music, last-watched movie, last-read book
- Auto-post when you start an audiobook (Audiobookshelf), finish an album (Navidrome), or star a track
- Discord confirmation flow before any auto-toot is posted
- Follower tracking with follow/unfollow history
- Weekly music recap post
- AI-powered roast of your posting habits
- OAuth login — no tokens to copy/paste
- Dark, responsive web UI
- Docker-ready

## Quick Start

```bash
git clone https://github.com/brunopatuleia/MastoFerr.git
cd MastoFerr
cp .env.example .env
docker compose up -d
```

Open `http://localhost:6886`, enter your Mastodon instance, and authorize.

See the [Wiki](https://github.com/brunopatuleia/MastoFerr/wiki) for full documentation.

## Tech Stack

Python 3.12 · FastAPI · SQLite FTS5 · Mastodon.py · APScheduler · Jinja2 · Docker

## License

MIT
