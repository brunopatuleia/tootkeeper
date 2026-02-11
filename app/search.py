import re
import sqlite3


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
        parts.append(f'"{phrase}"')
    for word in words:
        parts.append(f'"{word}"*')

    return " AND ".join(parts) if parts else ""


def search(
    conn: sqlite3.Connection,
    query: str,
    source_type: str = "",
    page: int = 1,
    per_page: int = 20,
) -> tuple[list[dict], int]:
    """Search the FTS index and return matching items with their source data."""
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
    search_params = [fts_query]
    if source_type:
        search_params.append(source_type)
    search_params.extend([per_page, offset])

    results_sql = f"""
        SELECT source_type, source_id, snippet(search_index, 2, '<mark>', '</mark>', '...', 40) as snippet,
               account, rank
        FROM search_index
        WHERE search_index MATCH ? {type_clause}
        ORDER BY rank
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(results_sql, search_params).fetchall()

    results = []
    for row in rows:
        item = dict(row)
        # Fetch the source record for additional context
        source = _get_source_record(conn, row["source_type"], row["source_id"])
        if source:
            item["source"] = source
        results.append(item)

    return results, total


def _get_source_record(conn: sqlite3.Connection, source_type: str, source_id: str) -> dict | None:
    table_map = {
        "toot": "toots",
        "notification": "notifications",
        "favorite": "favorites",
        "bookmark": "bookmarks",
    }
    table = table_map.get(source_type)
    if not table:
        return None
    row = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (source_id,)).fetchone()
    return dict(row) if row else None
