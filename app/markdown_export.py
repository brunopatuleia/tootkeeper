"""Export toots to human-readable Markdown files organized by year/month."""

import logging
import os
import sqlite3
from collections import defaultdict
from html.parser import HTMLParser
from pathlib import Path

from app.config import DB_PATH
from app.database import get_db, get_sync_state, set_sync_state

logger = logging.getLogger(__name__)

MARKDOWN_PATH = Path(DB_PATH).parent / "markdown"


class _Strip(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts = []
    def handle_data(self, data):
        self._parts.append(data)
    def handle_starttag(self, tag, attrs):
        if tag in ("br", "p"):
            self._parts.append("\n")


def _toot_to_markdown(toot: sqlite3.Row) -> str:
    """Format a single toot as a Markdown block."""
    date = toot["created_at"][:16].replace("T", " ")
    content = (toot["content_text"] or "").strip()

    if toot["reblog_id"]:
        reblog_account = toot["reblog_account"] or "unknown"
        lines = [f"## {date} *(boost from @{reblog_account})*", ""]
        if toot["reblog_content"]:
            p = _Strip()
            p.feed(toot["reblog_content"])
            reblog_text = "".join(p._parts).strip()
            for line in reblog_text.splitlines():
                lines.append(f"> {line}")
        lines += ["", "---", ""]
        return "\n".join(lines)

    lines = [f"## {date}", ""]
    if content:
        lines.append(content)
    lines += ["", "---", ""]
    return "\n".join(lines)


def export_new_toots(conn: sqlite3.Connection) -> int:
    """Append any unexported toots to their monthly Markdown files."""
    last_id = get_sync_state(conn, "markdown_last_exported_id") or "0"

    rows = conn.execute(
        """SELECT id, created_at, content_text, reblog_id, reblog_account, reblog_content
           FROM toots
           WHERE CAST(id AS INTEGER) > CAST(? AS INTEGER)
             AND reblog_id IS NULL OR reblog_id IS NOT NULL
           ORDER BY CAST(id AS INTEGER) ASC""",
        (last_id,),
    ).fetchall()

    if not rows:
        return 0

    MARKDOWN_PATH.mkdir(parents=True, exist_ok=True)
    newest_id = last_id

    # Group toots by target file to minimise open/close cycles
    toots_by_file: dict = defaultdict(list)
    for toot in rows:
        created = toot["created_at"] or ""
        year = created[:4]
        month = created[5:7]
        if not year or not month:
            continue
        filepath = MARKDOWN_PATH / year / f"{month}.md"
        toots_by_file[filepath].append(toot)
        if int(toot["id"]) > int(newest_id):
            newest_id = toot["id"]

    for filepath, toots in toots_by_file.items():
        filepath.parent.mkdir(exist_ok=True)
        if not filepath.exists():
            month_name = _month_name(int(filepath.stem))
            filepath.write_text(f"# {month_name} {filepath.parent.name}\n\n", encoding="utf-8")
        with filepath.open("a", encoding="utf-8") as f:
            f.write("".join(_toot_to_markdown(t) for t in toots))

    if newest_id != last_id:
        set_sync_state(conn, "markdown_last_exported_id", newest_id)

    logger.info(f"Markdown export: wrote {len(rows)} toots")
    return len(rows)


def _month_name(month: int) -> str:
    names = ["January", "February", "March", "April", "May", "June",
             "July", "August", "September", "October", "November", "December"]
    return names[month - 1] if 1 <= month <= 12 else str(month)
