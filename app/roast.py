"""AI-powered roast generation for Tootkeeper dashboard."""

import json
import logging
import sqlite3
import time
import urllib.request

from app.config import AI_API_KEY, AI_BASE_URL, AI_MODEL, AI_PROVIDER
from app.database import get_setting, set_setting

logger = logging.getLogger(__name__)


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


def _build_roast_prompt(stats: dict, history: list[str] = None) -> str:
    """Build the prompt to send to the AI for roast generation."""
    sample_text = "\n".join(f"- {t}" for t in stats.get("sample_recent_toots", [])[:10])
    if history:
        history_lines = "\n".join(f"- {h}" for h in history[-15:])
        history_section = f"IMPORTANT: Do NOT repeat or rephrase any of these previously used roasts:\n{history_lines}"
    else:
        history_section = ""
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

Write exactly 8-12 roast lines. Each line should be a standalone burn. Be savage but funny. Reference their actual content and habits. Include one line roasting them for building an app to archive all this.

{history_section}

Return ONLY the roast lines, one per line, no numbering, no bullets, no other text."""


def _call_ai_api(provider: str, api_key: str, model: str, base_url: str, prompt: str) -> str | None:
    """Call an AI API and return the response text. Returns None on failure."""
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
    provider = get_setting(conn, "ai_provider") or AI_PROVIDER
    api_key = get_setting(conn, "ai_api_key") or AI_API_KEY
    if not provider or not api_key:
        return None
    model = get_setting(conn, "ai_model") or AI_MODEL
    base_url = get_setting(conn, "ai_base_url") or AI_BASE_URL
    return provider, api_key, model, base_url


def _get_roast_history(conn: sqlite3.Connection) -> list[str]:
    """Get roast lines shown in the last 30 days."""
    raw = get_setting(conn, "roast_history")
    if not raw:
        return []
    try:
        entries = json.loads(raw)
        cutoff = time.time() - 30 * 86400
        valid = [e for e in entries if e.get("ts", 0) > cutoff]
        if len(valid) != len(entries):
            set_setting(conn, "roast_history", json.dumps(valid))
        return [e["text"] for e in valid]
    except (json.JSONDecodeError, TypeError):
        return []


def _add_to_roast_history(conn: sqlite3.Connection, text: str):
    """Add a roast line to the shown history."""
    raw = get_setting(conn, "roast_history")
    try:
        entries = json.loads(raw) if raw else []
    except (json.JSONDecodeError, TypeError):
        entries = []
    entries.append({"text": text, "ts": time.time()})
    entries = entries[-50:]
    set_setting(conn, "roast_history", json.dumps(entries))


def _fetch_roast_pool(conn: sqlite3.Connection, ai_config, stats, history) -> list[str]:
    """Call AI to get a fresh batch of roasts."""
    provider, api_key, model, base_url = ai_config
    prompt = _build_roast_prompt(stats, history=history)
    response = _call_ai_api(provider, api_key, model, base_url, prompt)
    if not response:
        return []
    lines = [line.strip() for line in response.strip().split("\n") if line.strip()]
    history_set = set(history)
    lines = [l for l in lines if l not in history_set]
    set_setting(conn, "roast_pool", json.dumps(lines))
    return lines


def generate_roast(conn: sqlite3.Connection, force: bool = False) -> str | None:
    """Return a single roast line. Returns None if AI not configured."""
    ai_config = _get_ai_config(conn)
    if not ai_config:
        return None

    if not force:
        current = get_setting(conn, "roast_current")
        if current:
            return current

    history = _get_roast_history(conn)

    pool_raw = get_setting(conn, "roast_pool")
    pool = []
    if pool_raw:
        try:
            pool = json.loads(pool_raw)
        except (json.JSONDecodeError, TypeError):
            pool = []

    history_set = set(history)
    available = [r for r in pool if r not in history_set]

    if not available:
        stats = _collect_roast_stats(conn)
        if stats["total_toots"] == 0:
            return None
        available = _fetch_roast_pool(conn, ai_config, stats, history)

    if not available:
        return None

    chosen = available[0]
    remaining = available[1:]
    set_setting(conn, "roast_pool", json.dumps(remaining))
    set_setting(conn, "roast_current", chosen)
    _add_to_roast_history(conn, chosen)

    return chosen
