import html
import re
import sqlite3
from collections import defaultdict


def _sanitize_fts_query(query: str) -> str:
    """Sanitize user input for FTS5 query syntax.

    Supports:
    - Quoted phrases: "exact phrase"
    - Plain words: each word is AND-ed
    - Strips special FTS operators to prevent syntax errors
    """
    if not query or not query.strip():
        return ""

    # Preserve quoted phrases
    phrases = re.findall(r'"([^"]+)"', query)
    remaining = re.sub(r'"[^"]*"', "", query).strip()

    # Split remaining words, strip FTS special chars
    words = []
    for word in remaining.split():
        cleaned = re.sub(r'[^\w]', '', word)
        if cleaned:
            words.append(cleaned)

    parts = []
    for phrase in phrases:
        # Escape internal double-quotes to prevent FTS5 syntax injection
        parts.append('"' + phrase.replace('"', '""') + '"')
    for word in words:
        parts.append(f'"{word}"*')

    return " AND ".join(parts) if parts else ""


_MAX_PAGE = 500  # Prevent runaway OFFSET queries (page * per_page ≤ 10 000 rows)

# Unique sentinels used as FTS5 highlight markers — replaced with safe <mark> tags after HTML-escaping
_MARK_S = "\x02MARKS\x02"
_MARK_E = "\x02MARKE\x02"


def search(
    conn: sqlite3.Connection,
    query: str,
    source_type: str = "",
    page: int = 1,
    per_page: int = 20,
) -> tuple[list[dict], int]:
    """Search the FTS index and return matching items with their source data."""
    page = min(page, _MAX_PAGE)
    fts_query = _sanitize_fts_query(query)
    if not fts_query:
        return [], 0

    params: list = [fts_query]
    type_clause = ""
    if source_type:
        type_clause = "AND source_type = ?"
        params.append(source_type)

    # Count total matches
    count_sql = f"""
        SELECT COUNT(*) as c FROM search_index
        WHERE search_index MATCH ? {type_clause}
    """
    total = conn.execute(count_sql, params).fetchone()["c"]

    # Get paginated results with snippets
    offset = (page - 1) * per_page
    search_params = [_MARK_S, _MARK_E, fts_query]
    if source_type:
        search_params.append(source_type)
    search_params.extend([per_page, offset])

    results_sql = f"""
        SELECT source_type, source_id, snippet(search_index, 2, ?, ?, '...', 40) as snippet,
               account, rank
        FROM search_index
        WHERE search_index MATCH ? {type_clause}
        ORDER BY rank
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(results_sql, search_params).fetchall()

    # Batch-fetch source records to avoid N+1 queries
    table_map = {
        "toot": "toots",
        "notification": "notifications",
        "favorite": "favorites",
        "bookmark": "bookmarks",
    }
    ids_by_type: dict[str, list] = defaultdict(list)
    for row in rows:
        ids_by_type[row["source_type"]].append(row["source_id"])

    sources_map: dict[tuple, dict] = {}
    for source_type, source_ids in ids_by_type.items():
        table = table_map.get(source_type)
        if not table:
            continue
        placeholders = ",".join("?" * len(source_ids))
        for s_row in conn.execute(f"SELECT * FROM {table} WHERE id IN ({placeholders})", source_ids):
            sources_map[(source_type, s_row["id"])] = dict(s_row)

    results = []
    for row in rows:
        item = dict(row)
        # HTML-escape the snippet then restore only the highlight markers as <mark> tags
        safe = html.escape(item.get("snippet") or "")
        safe = safe.replace(_MARK_S, "<mark>").replace(_MARK_E, "</mark>")
        item["snippet"] = safe
        source = sources_map.get((row["source_type"], row["source_id"]))
        if source:
            item["source"] = source
        results.append(item)

    return results, total
