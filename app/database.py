import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from html.parser import HTMLParser

from app.config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS toots (
    id TEXT PRIMARY KEY,
    created_at TEXT,
    content TEXT,
    content_text TEXT,
    url TEXT,
    in_reply_to_id TEXT,
    in_reply_to_account_id TEXT,
    reblog_id TEXT,
    reblog_content TEXT,
    reblog_account TEXT,
    favourites_count INTEGER DEFAULT 0,
    reblogs_count INTEGER DEFAULT 0,
    replies_count INTEGER DEFAULT 0,
    visibility TEXT,
    media_attachments TEXT,
    raw_json TEXT,
    fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY,
    type TEXT,
    created_at TEXT,
    account_id TEXT,
    account_acct TEXT,
    account_display_name TEXT,
    account_avatar TEXT,
    status_id TEXT,
    status_content TEXT,
    raw_json TEXT,
    fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS favorites (
    id TEXT PRIMARY KEY,
    created_at TEXT,
    content TEXT,
    content_text TEXT,
    url TEXT,
    account_id TEXT,
    account_acct TEXT,
    account_display_name TEXT,
    account_avatar TEXT,
    media_attachments TEXT,
    raw_json TEXT,
    favorited_at TEXT,
    fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS bookmarks (
    id TEXT PRIMARY KEY,
    created_at TEXT,
    content TEXT,
    content_text TEXT,
    url TEXT,
    account_id TEXT,
    account_acct TEXT,
    account_display_name TEXT,
    account_avatar TEXT,
    media_attachments TEXT,
    raw_json TEXT,
    bookmarked_at TEXT,
    fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_toots_created ON toots(created_at);
CREATE INDEX IF NOT EXISTS idx_notifications_created ON notifications(created_at);
CREATE INDEX IF NOT EXISTS idx_notifications_type ON notifications(type);
CREATE INDEX IF NOT EXISTS idx_favorites_created ON favorites(created_at);
CREATE INDEX IF NOT EXISTS idx_bookmarks_created ON bookmarks(created_at);
"""

FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
    source_type,
    source_id,
    content,
    account,
    tokenize='unicode61'
);
"""


class HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and extract plain text."""

    def __init__(self):
        super().__init__()
        self._text = []

    def handle_data(self, data):
        self._text.append(data)

    def handle_starttag(self, tag, attrs):
        if tag == "br":
            self._text.append("\n")
        elif tag == "p":
            self._text.append("\n")

    def get_text(self):
        return "".join(self._text).strip()


def html_to_text(html: str) -> str:
    if not html:
        return ""
    extractor = HTMLTextExtractor()
    extractor.feed(html)
    return extractor.get_text()


def _serialize_date(dt) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.isoformat()
    return str(dt)


def _serialize_json(obj) -> str:
    """Serialize Mastodon.py dict objects to JSON, handling datetime."""
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f"Object of type {type(o)} is not JSON serializable")
    return json.dumps(obj, default=default, ensure_ascii=False)


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript(SCHEMA)
        conn.executescript(FTS_SCHEMA)


def upsert_toot(conn: sqlite3.Connection, status: dict):
    now = datetime.now(timezone.utc).isoformat()
    reblog = status.get("reblog")
    reblog_id = None
    reblog_content = None
    reblog_account = None
    if reblog:
        reblog_id = str(reblog["id"])
        reblog_content = reblog.get("content", "")
        acct = reblog.get("account", {})
        reblog_account = acct.get("acct", acct.get("display_name", ""))

    content = status.get("content", "")
    content_text = html_to_text(content)
    if reblog_content:
        content_text += " " + html_to_text(reblog_content)

    media = status.get("media_attachments", [])
    media_json = _serialize_json(media) if media else "[]"

    conn.execute(
        """INSERT INTO toots
           (id, created_at, content, content_text, url, in_reply_to_id,
            in_reply_to_account_id, reblog_id, reblog_content, reblog_account,
            favourites_count, reblogs_count, replies_count, visibility,
            media_attachments, raw_json, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
            content=excluded.content, content_text=excluded.content_text,
            favourites_count=excluded.favourites_count,
            reblogs_count=excluded.reblogs_count,
            replies_count=excluded.replies_count,
            raw_json=excluded.raw_json, fetched_at=excluded.fetched_at""",
        (
            str(status["id"]),
            _serialize_date(status.get("created_at")),
            content,
            content_text,
            status.get("url"),
            str(status["in_reply_to_id"]) if status.get("in_reply_to_id") else None,
            str(status["in_reply_to_account_id"]) if status.get("in_reply_to_account_id") else None,
            reblog_id,
            reblog_content,
            reblog_account,
            status.get("favourites_count", 0),
            status.get("reblogs_count", 0),
            status.get("replies_count", 0),
            status.get("visibility"),
            media_json,
            _serialize_json(status),
            now,
        ),
    )

    # Update FTS index
    conn.execute("DELETE FROM search_index WHERE source_type='toot' AND source_id=?", (str(status["id"]),))
    conn.execute(
        "INSERT INTO search_index (source_type, source_id, content, account) VALUES (?, ?, ?, ?)",
        ("toot", str(status["id"]), content_text, ""),
    )


def upsert_notification(conn: sqlite3.Connection, notif: dict):
    now = datetime.now(timezone.utc).isoformat()
    account = notif.get("account", {})
    status = notif.get("status")
    status_id = str(status["id"]) if status else None
    status_content = status.get("content", "") if status else None

    conn.execute(
        """INSERT INTO notifications
           (id, type, created_at, account_id, account_acct, account_display_name,
            account_avatar, status_id, status_content, raw_json, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
            status_content=excluded.status_content,
            raw_json=excluded.raw_json, fetched_at=excluded.fetched_at""",
        (
            str(notif["id"]),
            notif.get("type"),
            _serialize_date(notif.get("created_at")),
            str(account.get("id", "")),
            account.get("acct", ""),
            account.get("display_name", ""),
            account.get("avatar", ""),
            status_id,
            status_content,
            _serialize_json(notif),
            now,
        ),
    )

    # Update FTS index
    content = html_to_text(status_content) if status_content else ""
    acct_name = account.get("acct", account.get("display_name", ""))
    conn.execute("DELETE FROM search_index WHERE source_type='notification' AND source_id=?", (str(notif["id"]),))
    conn.execute(
        "INSERT INTO search_index (source_type, source_id, content, account) VALUES (?, ?, ?, ?)",
        ("notification", str(notif["id"]), content, acct_name),
    )


def upsert_favorite(conn: sqlite3.Connection, status: dict):
    now = datetime.now(timezone.utc).isoformat()
    account = status.get("account", {})
    content = status.get("content", "")
    content_text = html_to_text(content)
    media = status.get("media_attachments", [])
    media_json = _serialize_json(media) if media else "[]"

    conn.execute(
        """INSERT INTO favorites
           (id, created_at, content, content_text, url, account_id, account_acct,
            account_display_name, account_avatar, media_attachments, raw_json,
            favorited_at, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
            content=excluded.content, content_text=excluded.content_text,
            raw_json=excluded.raw_json, fetched_at=excluded.fetched_at""",
        (
            str(status["id"]),
            _serialize_date(status.get("created_at")),
            content,
            content_text,
            status.get("url"),
            str(account.get("id", "")),
            account.get("acct", ""),
            account.get("display_name", ""),
            account.get("avatar", ""),
            media_json,
            _serialize_json(status),
            now,
            now,
        ),
    )

    acct_name = account.get("acct", account.get("display_name", ""))
    conn.execute("DELETE FROM search_index WHERE source_type='favorite' AND source_id=?", (str(status["id"]),))
    conn.execute(
        "INSERT INTO search_index (source_type, source_id, content, account) VALUES (?, ?, ?, ?)",
        ("favorite", str(status["id"]), content_text, acct_name),
    )


def upsert_bookmark(conn: sqlite3.Connection, status: dict):
    now = datetime.now(timezone.utc).isoformat()
    account = status.get("account", {})
    content = status.get("content", "")
    content_text = html_to_text(content)
    media = status.get("media_attachments", [])
    media_json = _serialize_json(media) if media else "[]"

    conn.execute(
        """INSERT INTO bookmarks
           (id, created_at, content, content_text, url, account_id, account_acct,
            account_display_name, account_avatar, media_attachments, raw_json,
            bookmarked_at, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
            content=excluded.content, content_text=excluded.content_text,
            raw_json=excluded.raw_json, fetched_at=excluded.fetched_at""",
        (
            str(status["id"]),
            _serialize_date(status.get("created_at")),
            content,
            content_text,
            status.get("url"),
            str(account.get("id", "")),
            account.get("acct", ""),
            account.get("display_name", ""),
            account.get("avatar", ""),
            media_json,
            _serialize_json(status),
            now,
            now,
        ),
    )

    acct_name = account.get("acct", account.get("display_name", ""))
    conn.execute("DELETE FROM search_index WHERE source_type='bookmark' AND source_id=?", (str(status["id"]),))
    conn.execute(
        "INSERT INTO search_index (source_type, source_id, content, account) VALUES (?, ?, ?, ?)",
        ("bookmark", str(status["id"]), content_text, acct_name),
    )


def get_sync_state(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM sync_state WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def set_sync_state(conn: sqlite3.Connection, key: str, value: str):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO sync_state (key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (key, value, now),
    )


def get_stats(conn: sqlite3.Connection) -> dict:
    stats = {}
    for table in ["toots", "notifications", "favorites", "bookmarks"]:
        row = conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()
        stats[table] = row["c"]
    return stats


def get_toots(conn: sqlite3.Connection, page: int = 1, per_page: int = 20) -> tuple[list, int]:
    offset = (page - 1) * per_page
    total = conn.execute("SELECT COUNT(*) as c FROM toots").fetchone()["c"]
    rows = conn.execute(
        "SELECT * FROM toots ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (per_page, offset),
    ).fetchall()
    return [dict(r) for r in rows], total


def get_notifications(conn: sqlite3.Connection, page: int = 1, per_page: int = 20, type_filter: str = "") -> tuple[list, int]:
    if type_filter:
        total = conn.execute("SELECT COUNT(*) as c FROM notifications WHERE type=?", (type_filter,)).fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM notifications WHERE type=? ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (type_filter, per_page, (page - 1) * per_page),
        ).fetchall()
    else:
        total = conn.execute("SELECT COUNT(*) as c FROM notifications").fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM notifications ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (per_page, (page - 1) * per_page),
        ).fetchall()
    return [dict(r) for r in rows], total


def get_favorites(conn: sqlite3.Connection, page: int = 1, per_page: int = 20) -> tuple[list, int]:
    offset = (page - 1) * per_page
    total = conn.execute("SELECT COUNT(*) as c FROM favorites").fetchone()["c"]
    rows = conn.execute(
        "SELECT * FROM favorites ORDER BY favorited_at DESC LIMIT ? OFFSET ?",
        (per_page, offset),
    ).fetchall()
    return [dict(r) for r in rows], total


def get_bookmarks(conn: sqlite3.Connection, page: int = 1, per_page: int = 20) -> tuple[list, int]:
    offset = (page - 1) * per_page
    total = conn.execute("SELECT COUNT(*) as c FROM bookmarks").fetchone()["c"]
    rows = conn.execute(
        "SELECT * FROM bookmarks ORDER BY bookmarked_at DESC LIMIT ? OFFSET ?",
        (per_page, offset),
    ).fetchall()
    return [dict(r) for r in rows], total


def get_toot_detail(conn: sqlite3.Connection, toot_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM toots WHERE id=?", (toot_id,)).fetchone()
    if not row:
        return None
    toot = dict(row)
    # Get related notifications
    notifs = conn.execute(
        "SELECT * FROM notifications WHERE status_id=? ORDER BY created_at DESC",
        (toot_id,),
    ).fetchall()
    toot["notifications"] = [dict(n) for n in notifs]
    return toot


def get_setting(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def set_setting(conn: sqlite3.Connection, key: str, value: str):
    conn.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def get_all_settings(conn: sqlite3.Connection) -> dict:
    rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
    return {r["key"]: r["value"] for r in rows}


def is_configured(conn: sqlite3.Connection) -> bool:
    token = get_setting(conn, "access_token")
    instance = get_setting(conn, "instance_url")
    return bool(token and instance)


def _collect_roast_stats(conn: sqlite3.Connection) -> dict:
    """Collect posting stats for roast generation."""
    total_toots = conn.execute("SELECT COUNT(*) as c FROM toots").fetchone()["c"]
    total_favs = conn.execute("SELECT COUNT(*) as c FROM favorites").fetchone()["c"]
    total_bookmarks = conn.execute("SELECT COUNT(*) as c FROM bookmarks").fetchone()["c"]
    total_notifs = conn.execute("SELECT COUNT(*) as c FROM notifications").fetchone()["c"]

    if total_toots == 0:
        return {"total_toots": 0}

    boosts = conn.execute("SELECT COUNT(*) as c FROM toots WHERE reblog_id IS NOT NULL").fetchone()["c"]
    original_toots = total_toots - boosts
    replies = conn.execute("SELECT COUNT(*) as c FROM toots WHERE in_reply_to_id IS NOT NULL AND reblog_id IS NULL").fetchone()["c"]
    zero_engagement = conn.execute(
        "SELECT COUNT(*) as c FROM toots WHERE reblog_id IS NULL AND favourites_count = 0 AND reblogs_count = 0 AND replies_count = 0"
    ).fetchone()["c"]
    avg_len = conn.execute(
        "SELECT AVG(LENGTH(content_text)) as avg_len FROM toots WHERE reblog_id IS NULL AND content_text IS NOT NULL AND content_text != ''"
    ).fetchone()["avg_len"]
    night_toots = conn.execute(
        "SELECT COUNT(*) as c FROM toots WHERE CAST(SUBSTR(created_at, 12, 2) AS INTEGER) BETWEEN 0 AND 5"
    ).fetchone()["c"]
    unlisted = conn.execute("SELECT COUNT(*) as c FROM toots WHERE visibility = 'unlisted'").fetchone()["c"]
    fav_notifs = conn.execute("SELECT COUNT(*) as c FROM notifications WHERE type='favourite'").fetchone()["c"]
    reblog_notifs = conn.execute("SELECT COUNT(*) as c FROM notifications WHERE type='reblog'").fetchone()["c"]
    follow_notifs = conn.execute("SELECT COUNT(*) as c FROM notifications WHERE type='follow'").fetchone()["c"]
    mention_notifs = conn.execute("SELECT COUNT(*) as c FROM notifications WHERE type='mention'").fetchone()["c"]

    # Sample recent toots for content analysis
    recent_toots = conn.execute(
        "SELECT content_text FROM toots WHERE reblog_id IS NULL AND content_text IS NOT NULL AND content_text != '' ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    sample_content = [r["content_text"][:200] for r in recent_toots]

    return {
        "total_toots": total_toots,
        "total_favorites": total_favs,
        "total_bookmarks": total_bookmarks,
        "total_notifications": total_notifs,
        "boosts": boosts,
        "boost_pct": round(boosts / total_toots * 100),
        "original_toots": original_toots,
        "replies": replies,
        "reply_pct": round(replies / original_toots * 100) if original_toots else 0,
        "zero_engagement_count": zero_engagement,
        "zero_engagement_pct": round(zero_engagement / original_toots * 100) if original_toots else 0,
        "avg_toot_length": round(avg_len) if avg_len else 0,
        "night_toots_pct": round(night_toots / total_toots * 100),
        "unlisted_pct": round(unlisted / total_toots * 100),
        "fav_notifications": fav_notifs,
        "reblog_notifications": reblog_notifs,
        "follow_notifications": follow_notifs,
        "mention_notifications": mention_notifs,
        "sample_recent_toots": sample_content,
    }


def _build_roast_prompt(stats: dict) -> str:
    """Build the prompt to send to the AI for roast generation."""
    sample_text = "\n".join(f"- {t}" for t in stats.get("sample_recent_toots", [])[:10])
    return f"""You are a savage comedy roast writer. Analyze this Mastodon user's posting stats and write a brutal, hilarious roast. Be creative, specific, and merciless. Don't hold back.

STATS:
- Total toots: {stats['total_toots']} ({stats['boost_pct']}% are boosts, {stats['reply_pct']}% are replies)
- Original toots: {stats['original_toots']}
- Favorites given: {stats['total_favorites']}
- Bookmarks saved: {stats['total_bookmarks']}
- {stats['zero_engagement_pct']}% of original toots got ZERO engagement (no likes, no boosts, no replies)
- Average toot length: {stats['avg_toot_length']} characters
- {stats['night_toots_pct']}% of toots posted between midnight and 5 AM
- {stats['unlisted_pct']}% of toots are unlisted
- Notifications received: {stats['fav_notifications']} favorites, {stats['reblog_notifications']} boosts, {stats['follow_notifications']} follows, {stats['mention_notifications']} mentions

SAMPLE RECENT TOOTS:
{sample_text}

Write exactly 5-8 roast lines. Each line should be a standalone burn. Be savage but funny. Reference their actual content and habits. End with one line roasting them for building an app to archive all this.

Return ONLY the roast lines, one per line, no numbering, no bullets, no other text."""


def _call_ai_api(provider: str, api_key: str, model: str, base_url: str, prompt: str) -> str | None:
    """Call an AI API and return the response text. Returns None on failure."""
    import urllib.request
    import urllib.error

    try:
        if provider == "anthropic":
            url = "https://api.anthropic.com/v1/messages"
            model = model or "claude-sonnet-4-5-20250929"
            payload = json.dumps({
                "model": model,
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
            })
            req = urllib.request.Request(url, data=payload.encode(), headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                return data["content"][0]["text"]

        elif provider in ("openai", "openai-compatible"):
            url = (base_url.rstrip("/") if base_url else "https://api.openai.com/v1") + "/chat/completions"
            model = model or ("gpt-4o" if provider == "openai" else "llama3")
            payload = json.dumps({
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1024,
            })
            headers = {"Content-Type": "application/json"}
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"
            req = urllib.request.Request(url, data=payload.encode(), headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                return data["choices"][0]["message"]["content"]

        elif provider == "gemini":
            model = model or "gemini-2.0-flash"
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
            payload = json.dumps({
                "contents": [{"parts": [{"text": prompt}]}],
            })
            req = urllib.request.Request(url, data=payload.encode(), headers={
                "Content-Type": "application/json",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                return data["candidates"][0]["content"]["parts"][0]["text"]

    except Exception as e:
        logger.error(f"AI API call failed ({provider}): {e}")
        return None


def _get_ai_config(conn: sqlite3.Connection) -> tuple[str, str, str, str] | None:
    """Get AI provider config from DB settings, falling back to env vars."""
    from app.config import AI_PROVIDER, AI_API_KEY, AI_MODEL, AI_BASE_URL
    provider = get_setting(conn, "ai_provider") or AI_PROVIDER
    api_key = get_setting(conn, "ai_api_key") or AI_API_KEY
    if not provider or not api_key:
        return None
    model = get_setting(conn, "ai_model") or AI_MODEL
    base_url = get_setting(conn, "ai_base_url") or AI_BASE_URL
    return provider, api_key, model, base_url


def generate_roast(conn: sqlite3.Connection, force: bool = False) -> list[str]:
    """Generate a roast using AI. Returns cached version if available. Returns [] if AI not configured."""
    import time

    # Check if AI is configured
    ai_config = _get_ai_config(conn)
    if not ai_config:
        return []

    # Check cache (valid for 24 hours)
    if not force:
        cached = get_setting(conn, "roast_cache")
        cache_time = get_setting(conn, "roast_cache_time")
        if cached and cache_time:
            try:
                if time.time() - float(cache_time) < 86400:
                    return json.loads(cached)
            except (ValueError, json.JSONDecodeError):
                pass

    # Collect stats
    stats = _collect_roast_stats(conn)
    if stats["total_toots"] == 0:
        return []

    # Call AI
    provider, api_key, model, base_url = ai_config
    prompt = _build_roast_prompt(stats)
    response = _call_ai_api(provider, api_key, model, base_url, prompt)

    if not response:
        return []

    # Parse response into lines
    lines = [line.strip() for line in response.strip().split("\n") if line.strip()]

    # Cache the result
    set_setting(conn, "roast_cache", json.dumps(lines))
    set_setting(conn, "roast_cache_time", str(time.time()))

    return lines


def get_topic_counts(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    """Extract common topics/words from toot content, excluding stopwords and short words."""
    from collections import Counter
    import re

    stopwords = {
        "the", "and", "for", "are", "but", "not", "you", "all", "can", "had", "her",
        "was", "one", "our", "out", "has", "have", "been", "from", "this", "that",
        "with", "they", "will", "each", "make", "like", "just", "over", "such", "take",
        "than", "them", "very", "some", "what", "when", "who", "how", "its", "also",
        "into", "about", "more", "other", "which", "their", "there", "would", "could",
        "should", "these", "those", "then", "being", "here", "where", "does", "done",
        "doing", "going", "were", "went", "your", "it's", "don't", "i'm", "it",
        "de", "que", "um", "uma", "para", "com", "por", "mais", "mas", "como",
        "dos", "das", "nos", "nas", "aos", "seu", "sua", "esse", "essa", "isso",
        "este", "esta", "isto", "ele", "ela", "eles", "elas", "nao", "sim", "bem",
        "muito", "tambem", "ainda", "depois", "antes", "sobre", "entre", "mesmo",
        "quando", "onde", "quem", "qual", "cada", "todo", "toda", "todos", "todas",
        "http", "https", "www", "com",
    }
    counts = Counter()
    for table in ("toots", "favorites", "bookmarks"):
        rows = conn.execute(f"SELECT content_text FROM {table} WHERE content_text IS NOT NULL AND content_text != ''").fetchall()
        for row in rows:
            words = re.findall(r'[a-zA-ZÀ-ÿ]{4,}', row["content_text"].lower())
            for word in words:
                if word not in stopwords and not word.startswith("http"):
                    counts[word] += 1
    # Filter to words appearing at least 3 times
    filtered = {w: c for w, c in counts.items() if c >= 3}
    most_common = Counter(filtered).most_common(limit)
    if not most_common:
        return []
    max_count = most_common[0][1]
    min_count = most_common[-1][1]
    result = []
    for name, count in most_common:
        if max_count == min_count:
            weight = 3
        else:
            weight = 1 + 4 * (count - min_count) / (max_count - min_count)
        result.append({"name": name, "count": count, "weight": round(weight, 2)})
    result.sort(key=lambda t: t["count"], reverse=True)
    return result


def get_hashtag_counts(conn: sqlite3.Connection, limit: int = 100) -> list[dict]:
    """Extract hashtag counts from raw_json across toots, favorites, and bookmarks."""
    from collections import Counter
    counts = Counter()
    for table in ("toots", "favorites", "bookmarks"):
        rows = conn.execute(f"SELECT raw_json FROM {table} WHERE raw_json IS NOT NULL").fetchall()
        for row in rows:
            try:
                data = json.loads(row["raw_json"])
                tags = data.get("tags", [])
                # Also check reblog tags
                reblog = data.get("reblog")
                if reblog and isinstance(reblog, dict):
                    tags = tags + reblog.get("tags", [])
                for tag in tags:
                    if isinstance(tag, dict) and tag.get("name"):
                        counts[tag["name"].lower()] += 1
            except (json.JSONDecodeError, TypeError):
                continue
    most_common = counts.most_common(limit)
    if not most_common:
        return []
    max_count = most_common[0][1]
    min_count = most_common[-1][1]
    result = []
    for name, count in most_common:
        if max_count == min_count:
            weight = 3
        else:
            weight = 1 + 4 * (count - min_count) / (max_count - min_count)
        result.append({"name": name, "count": count, "weight": round(weight, 2)})
    result.sort(key=lambda t: t["count"], reverse=True)
    return result
