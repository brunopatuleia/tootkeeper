# Tootkeeper

A self-hosted Mastodon activity archiver with full-text search. Automatically saves your toots, notifications, favorites, bookmarks, and media attachments to a local SQLite database.

> Built entirely through vibe coding with [Claude Code](https://docs.anthropic.com/en/docs/claude-code).

## Features

- **Archive everything** - Toots, boosts, replies, notifications, favorites, bookmarks
- **Media downloads** - Saves images locally so they're preserved even if the original is deleted
- **Full-text search** - SQLite FTS5 powered search across all your archived content
- **Hashtag & topic clouds** - See your most-used hashtags and topics at a glance
- **Profile updater** - Auto-update your Mastodon profile fields with now-playing music (Last.fm/ListenBrainz/Navidrome), last-watched movie (Letterboxd), and last-read book (Goodreads)
- **AI-powered roast** - Optional AI roast on your dashboard that roasts your posting habits (supports Anthropic, OpenAI, Gemini, Ollama)
- **OAuth login** - No tokens to copy/paste, just enter your instance and authorize
- **Automatic sync** - Polls for new activity every 5 minutes (configurable)
- **Tools & Settings** - Configure profile updater, AI provider, manage account, all from the web UI
- **Dark UI** - Clean, responsive web interface
- **Any instance** - Works with any Mastodon-compatible server
- **Docker-ready** - Single container, just `docker compose up`

## Installation

### Step 1: Install Docker

**Linux (Debian/Ubuntu):**

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh

# Add your user to the docker group (so you can run docker without sudo)
sudo usermod -aG docker $USER

# Log out and back in for the group change to take effect, then verify:
docker --version
```

**Windows:** Download and install [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)

**Mac:** Download and install [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)

### Step 2: Download Tootkeeper

```bash
git clone https://github.com/brunopatuleia/tootkeeper.git
cd tootkeeper
```

### Step 3: Configure

```bash
# Create your config file from the example
cp .env.example .env
```

Edit `.env` if you need to change the default settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_URL` | `http://localhost:6886` | The URL where Tootkeeper is reachable (important for OAuth) |
| `POLL_INTERVAL` | `5` | How often to check for new activity (in minutes) |
| `DB_PATH` | `/app/data/tootkeeper.db` | Where the database is stored inside the container |
| `MEDIA_PATH` | `/app/data/media` | Where downloaded images are stored inside the container |
| `AI_PROVIDER` | *(disabled)* | AI provider for roast: `anthropic`, `openai`, `gemini`, or `openai-compatible` |
| `AI_API_KEY` | | Your AI provider API key |
| `AI_MODEL` | *(auto)* | Model to use (e.g. `claude-sonnet-4-5-20250929`, `gpt-4o`, `gemini-2.0-flash`) |
| `AI_BASE_URL` | | Only for `openai-compatible` (e.g. `http://localhost:11434/v1` for Ollama) |

**Important:** If you run the app on a different port (e.g. `8080`), you **must** update `APP_URL` (e.g. `http://localhost:8080`) so the OAuth login redirects back to the correct place.

AI settings can also be configured from the **Settings** page in the web UI.

If you're running on a remote server, set `APP_URL` to the server's address (e.g. `http://your-server-ip:6886`).

### Step 4: Start Tootkeeper

```bash
docker compose up -d
```

This builds the container and starts it in the background. First run may take a minute to download dependencies.

### Step 5: Connect your Mastodon account

1. Open `http://localhost:6886` in your browser (or your server's IP)
2. Enter your Mastodon instance domain (e.g. `mastodon.social`, `fosstodon.org`)
3. Click **Login with Mastodon**
4. You'll be redirected to your instance to authorize access (read + write:accounts for profile updates)
5. After authorizing, Tootkeeper starts archiving your full history immediately

That's it! Tootkeeper will continue syncing new activity every 5 minutes.

### Updating

```bash
cd tootkeeper
git pull
docker compose up --build -d
```

### Headless / Automated Setup

If you prefer not to use the OAuth flow, you can set credentials directly in `.env`:

```bash
MASTODON_INSTANCE=https://mastodon.social
MASTODON_ACCESS_TOKEN=your_access_token_here
```

To get an access token, go to your instance's **Preferences > Development > New application**, create an app with `read write:accounts` scopes, and copy the access token.

## Profile Updater (Tools Tab)

The **Tools** tab lets you automatically update your Mastodon profile fields with what you're currently consuming. It supports three media sources plus a custom user-defined field:

### Music (Last.fm / ListenBrainz / Navidrome)

Shows what you're currently listening to or your most recent track. Supports three music services with automatic fallback — if one is down, the next takes over. Checks every 60 seconds by default. When nothing is playing, the last played track stays on your profile.

**Example:** 🎵 Radiohead - Karma Police

### Movies (Letterboxd)

Shows the last movie you watched, pulled from your public Letterboxd RSS feed. Includes the year and your star rating if you rated it. Checks every 6 hours by default.

**Example:** 🎬 Oppenheimer (2023) - ★★★★½

### Books (Goodreads)

Shows the last book you finished and rated, pulled from your Goodreads RSS feed. Includes the author and your star rating. Checks every 6 hours by default.

**Example:** 📚 Dune by Frank Herbert - ★★★★★

### Custom Field

A user-defined field that you can set to any static text — useful for a status, a link, or anything else you want to display on your profile.

### Setup

Each source can be individually enabled or disabled with a checkbox in the Tools page. Just enter your usernames, API keys, and RSS feed URLs for the sources you want to use. The updater runs as a background thread and only updates your profile when the content actually changes.

> **Note:** Mastodon profiles allow a maximum of **4 metadata fields**. The profile updater can use up to 3 of those fields (music, movies, books) plus 1 custom field — which means it could occupy all 4 slots. Keep this in mind if you also use profile fields for other things like your website or pronouns. You can enable only the sources you need to leave room for your other fields.

## What Gets Archived

| Data | Details |
|------|---------|
| **Your toots** | Posts, replies, boosts |
| **Notifications** | Likes, boosts, mentions, follows on your toots |
| **Favorites** | Toots you've liked |
| **Bookmarks** | Toots you've bookmarked |
| **Media** | Images and GIFs from all of the above, stored locally |

## Tech Stack

- Python 3.12 + FastAPI
- SQLite with FTS5 full-text search
- Mastodon.py
- APScheduler
- Jinja2 templates
- Docker

## License

MIT
