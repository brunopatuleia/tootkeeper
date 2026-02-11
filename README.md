# Tootkeeper

A self-hosted Mastodon activity archiver with full-text search. Automatically saves your toots, notifications, favorites, bookmarks, and media attachments to a local SQLite database.

## Features

- **Archive everything** - Toots, boosts, replies, notifications, favorites, bookmarks
- **Media downloads** - Saves images locally so they're preserved even if the original is deleted
- **Full-text search** - SQLite FTS5 powered search across all your archived content
- **OAuth login** - No tokens to copy/paste, just enter your instance and authorize
- **Automatic sync** - Polls for new activity every 5 minutes (configurable)
- **Dark UI** - Clean, responsive web interface
- **Any instance** - Works with any Mastodon-compatible server
- **Docker-ready** - Single container, just `docker compose up`

## Quick Start

```bash
git clone https://github.com/brunopatuleia/tootkeeper.git
cd tootkeeper
cp .env.example .env
docker compose up -d
```

Open `http://localhost:8080`, enter your Mastodon instance domain, and authorize. Tootkeeper will immediately start archiving your full history.

## Configuration

All configuration is in `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_URL` | `http://localhost:8080` | External URL for OAuth redirect |
| `POLL_INTERVAL` | `5` | Sync interval in minutes |
| `DB_PATH` | `/app/data/tootkeeper.db` | SQLite database path |
| `MEDIA_PATH` | `/app/data/media` | Downloaded media storage path |

For headless/automated setups, you can optionally set `MASTODON_INSTANCE` and `MASTODON_ACCESS_TOKEN` instead of using the OAuth flow.

## What Gets Archived

| Data | Source | Details |
|------|--------|---------|
| Your toots | `/api/v1/accounts/:id/statuses` | Posts, replies, boosts |
| Notifications | `/api/v1/notifications` | Likes, boosts, mentions, follows on your toots |
| Favorites | `/api/v1/favourites` | Toots you've liked |
| Bookmarks | `/api/v1/bookmarks` | Toots you've bookmarked |
| Media | Attachment URLs | Images and GIFs from all of the above |

## Tech Stack

- Python 3.12 + FastAPI
- SQLite with FTS5 full-text search
- Mastodon.py
- APScheduler
- Jinja2 templates
- Docker

## License

MIT
