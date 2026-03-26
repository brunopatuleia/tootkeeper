# Changelog

All notable changes to Mastoferr are documented here.

> This project is built entirely through vibe coding with [Claude Code](https://claude.ai/claude-code). Every feature, fix, and security patch in this changelog was written by AI.

---

## [Unreleased]

### Added
- GitHub wiki with full documentation

---

## [2026-03-26]

### Added
- libre.fm as a music source for the profile updater and auto-toots (no API key required)
- `%SimilarArtists%` variable in the Loved Track template — pulls top 3 similar artists from Last.fm `artist.getSimilar`
- Clickable variable chips in all template editors — click to insert `%Variable%` at the cursor
- Profile links on the Followers / Users page unfollowers table and event cards
- "Followed at" date column in the unfollowers table
- Mobile-responsive navigation: hamburger menu, collapsible nav, responsive tables

### Fixed
- Duplicate toots on settings save: old profile updater thread was not waited for before spawning a new one
- Duplicate album toots: double-check locking pattern prevents two threads posting simultaneously

### Security
- `/confirm-toot` XSS follow-up: previous fix created `safe_label` but the `HTMLResponse` still used the unescaped `label`; corrected to use `html.escape()` inline

### Refactored
- Applied code review fixes (simplify pass): module-level `APP_URL` import, `vars` → `substitutions` rename, guard expensive Last.fm/Odesli API calls behind template content checks, unified `LastFmClient`/`LibreFmClient` into `_LastFmCompatClient` base class, SQL `HAVING` clause replaces subquery in `get_unfollowers()`

---

## [2026-03-23]

### Added
- ABS toot format: subtitle, narrator, and year included when available
- ABS share link: optionally append a public share link to audiobook toots (opt-in via public URL + expiry hours setting)
- Discord confirmation flow: queue any auto-toot and post only after you approve it via a Discord webhook message
- Info tooltips `(i)` on every Auto Toots settings option

### Fixed
- Genre hashtags now strip hyphens, ampersands, and all special characters — `Avant-garde Metal` → `#AvantGardeMetal`
- Album listening session now persisted to the database and survives app restarts and redeploys
- Tooltip text was invisible due to undefined `--text-primary` CSS variable (corrected to `--text`)

### Security
- Reflected XSS in `/confirm-toot` response: toot label HTML-escaped before rendering
- Path traversal in media download: `media_id` sanitized to `[a-zA-Z0-9_-]` before use as filename
- Unsafe file types blocked on media download: only allowlisted extensions (jpg, png, gif, webp, mp4, mp3, etc.) written to disk
- FTS5 search snippet XSS: snippets HTML-escaped server-side via sentinel markers; `| safe` removed from template
- Discord webhook URL validated against `discord.com/api/webhooks` allowlist (SSRF)
- `/auth/logout` now requires an active session (CSRF prevention)
- `/confirm-toot` rate-limited to 10 requests per 60 seconds per IP
- Raw exception message removed from `/confirm-toot` error response
- Session and OAuth state cookies now use `Secure` flag when `APP_URL` is `https://`

---

## [2026-03-20]

### Added
- Backup page: download SQLite database and Markdown archive
- Backup page: filtered data export (by type, date range, or search query) as JSON or CSV
- Audiobookshelf: toot when you finish an audiobook
- Friends or Stalkers interactions page
- Interactions page: likes and boosts tables, configurable time window, period selector
- Hashtags and Topics pages: period dropdown to filter by time range
- Sync filtered notification requests (Mastodon 4.3+ filtered inbox)

### Fixed
- Video and audio attachments now downloaded alongside images
- Goodreads RSS parsing: use entry title instead of HTML classes
- Interactions page: display name mismatch in "you reply to them"
- Favorites and bookmarks sorted newest to oldest by post date

### Refactored
- Applied Gemini Code Assist code review suggestions (two rounds)

---

## [2026-03-19]

### Added
- Rename: TootKeeper → **Mastoferr**
- Redesigned homepage dashboard and navbar
- Markdown backup of toots (organized by year/month, updated on every sync)
- Navidrome loved track posts: toot with cover art when you star a track
- AI roast: like/dislike feedback — liked roasts are used as future style examples, disliked roasts are permanently blacklisted
- AI roast: "Toot This" opens Mastodon compose window pre-filled (no auto-posting)
- Toast notifications on roast rating

### Fixed
- Favorites/bookmarks infinite loop in pagination
- Navidrome star toot first-run seeding (no false positive on first sync)
- Disliked roasts correctly blacklisted from future rotation
- Corrupted code in `collector.py` and `sync_favorites` from prior AI edit

---

## [2026-03-18]

### Added
- Follower tracking: follow/unfollow history, line chart, unfollowers table

### Fixed
- Album toot threshold corrected to 65%, track order strictly enforced
- Album detection always polls Navidrome directly (no cache race)
- OAuth scopes updated to include `write:statuses` and `write:media`

---

## [2026-03-16]

### Added
- Spotify, Jellyfin, Plex, and Tautulli as additional music sources for profile updater
- Settings reorganized into Profile Fields and Auto Toots panels
- External Services configuration page

### Security
- OAuth state CSRF protection (token generated, verified, and cleared per login)
- SSRF validation strengthened across all user-supplied URL fields
- Prompt injection mitigation: toot content wrapped in XML delimiter tags before AI insertion

---

## [2026-03-15]

### Added
- Audiobookshelf integration: toot with cover art when starting a new audiobook
- Optional password protection for the web UI (`APP_PASSWORD`)
- Album listening posts (Navidrome): toot when ≥65% of an album is heard in track order

### Fixed
- Data volume ownership auto-fixed at container startup
- Handles missing DB tables gracefully on first boot

---

## [2026-03-14]

### Added
- Weekly top 5 artists toot (Last.fm, ListenBrainz, or Navidrome), every Monday
- Goodreads book activity posts: toot when starting or finishing a book
- Customizable hashtags for book posts and weekly recap

### Fixed
- Favorites/bookmarks sync cursor derived from fetched item IDs (not pagination headers)
- GitHub version check result cached for 1 hour
- Explicit `rollback()` added to database context manager
- Mastodon API rate limit pressure reduced

---

## [2026-03-13]

### Security
- Container runs as non-root user (`appuser`, UID 1000) with health check
- Memory capped at 512 MB, CPU at 1 core in Docker Compose
- AI roast endpoint rate-limited to 1 request per 30 seconds
- OAuth error messages URL-encoded before embedding in redirect URLs
- Search page numbers capped to prevent runaway SQLite offset queries
- Open redirect fixed in login flow — `?next=` validates relative paths only
- OAuth registration endpoint protected from unauthenticated access

---

## [2026-02-22]

### Fixed
- Last track, movie, and book persisted across app restarts

---

## [2026-02-15 – 2026-02-17]

### Added
- Navidrome support in profile updater
- Version check with update notification in footer
- Mastodon-style sidebar navigation in Settings
- Per-source toggles, custom profile field, drag-to-sort field order
- Pagination improvements in collector

---

## [2026-02-11 – 2026-02-13]

### Added
- Initial release as **TootKeeper**
- Archive toots, notifications, favorites, bookmarks, and media
- Full-text search (SQLite FTS5)
- Hashtag and topic clouds
- AI-powered dashboard roast (Anthropic, OpenAI, Gemini, Ollama)
- Settings page
- Profile updater (Last.fm, ListenBrainz, Letterboxd, Goodreads)
- OAuth login
- Docker support
