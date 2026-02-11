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
