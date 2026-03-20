import logging
import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

from mastodon import Mastodon

from app.config import MASTODON_ACCESS_TOKEN, MASTODON_INSTANCE, MEDIA_PATH
from app.database import (
    get_db,
    get_setting,
    get_sync_state,
    set_sync_state,
    upsert_bookmark,
    upsert_favorite,
    upsert_notification,
    upsert_toot,
)

logger = logging.getLogger(__name__)


def get_client() -> Mastodon:
    """Build a Mastodon client using DB credentials, falling back to env vars."""
    with get_db() as conn:
        instance = get_setting(conn, "instance_url") or MASTODON_INSTANCE
        token = get_setting(conn, "access_token") or MASTODON_ACCESS_TOKEN

    if not instance or not token:
        raise RuntimeError("Mastodon credentials not configured")

    return Mastodon(
        access_token=token,
        api_base_url=instance,
        ratelimit_method="pace",   # proactively slow down before hitting the limit
        ratelimit_pacefactor=0.9,  # stay at 90% of the allowed rate
    )


_BLOCKED_HOSTS = {"169.254.169.254", "169.254.170.2", "metadata.google.internal"}


def _safe_media_url(url: str) -> bool:
    """Block loopback and cloud metadata URLs before downloading media."""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        hostname = (parsed.hostname or "").lower()
        if hostname in _BLOCKED_HOSTS:
            return False
        try:
            ip = socket.gethostbyname(hostname)
            if ip.startswith("127.") or ip in ("0.0.0.0", "::1"):
                return False
        except socket.gaierror:
            return False
        return True
    except Exception:
        return False


def _get_extension(url: str) -> str:
    """Extract file extension from URL."""
    path = urlparse(url).path
    ext = os.path.splitext(path)[1]
    return ext.lower() if ext else ".jpg"


def download_media(status: dict):
    """Download media attachments from a status to local storage."""
    media_list = status.get("media_attachments", [])
    if not media_list:
        return

    # Also handle boosts
    reblog = status.get("reblog")
    if reblog:
        download_media(reblog)

    for media in media_list:
        if not isinstance(media, dict):
            continue
        media_id = str(media.get("id", ""))
        if not media_id:
            continue

        media_type = media.get("type", "image")
        if media_type not in ("image", "gifv", "video", "audio"):
            continue

        url = media.get("url") or media.get("remote_url")
        preview_url = media.get("preview_url")

        if url and _safe_media_url(url):
            ext = _get_extension(url)
            local_path = Path(MEDIA_PATH) / f"{media_id}{ext}"
            if not local_path.exists():
                _download_file(url, local_path)

        if preview_url and _safe_media_url(preview_url):
            ext = _get_extension(preview_url)
            preview_path = Path(MEDIA_PATH) / f"{media_id}_preview{ext}"
            if not preview_path.exists():
                _download_file(preview_url, preview_path)


def _download_file(url: str, dest: Path):
    """Download a file from URL to local path."""
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        resp = requests.get(url, timeout=30, stream=True)
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception:
        logger.debug(f"Failed to download {url}")


def _fetch_all_pages(fetch_func, client=None, since_id=None, limit=40, stop_on_id=True):
    """Fetch all pages from a paginated Mastodon API endpoint.

    When since_id is None (first historical sync), fetches everything with no page limit.
    When since_id is set (incremental sync), only fetches items newer than since_id.
    """
    all_items = []
    kwargs = {"limit": limit}
    if since_id and stop_on_id:
        kwargs["since_id"] = since_id

    page = fetch_func(**kwargs)
    if not page:
        return all_items, None

    # Try to get new cursor (min_id) from the first page
    new_cursor = None
    if isinstance(page, list) and hasattr(page, '_pagination_prev'):
        new_cursor = page._pagination_prev.get('min_id')

    all_items.extend(page)
    pages_fetched = 1

    while page:
        if client:
            if hasattr(page, '_pagination_next'):
                page = client.fetch_next(page)
            else:
                page = []
        else:
            prev_page = page
            next_max_id = None
            if isinstance(prev_page, list) and hasattr(prev_page, '_pagination_next'):
                next_max_id = prev_page._pagination_next.get('max_id')
            
            if next_max_id:
                page = fetch_func(max_id=next_max_id, limit=limit)
            else:
                page = fetch_func(max_id=int(prev_page[-1]["id"]) - 1, limit=limit)

        if not page:
            break
        # If we have a since_id and go past it, stop
        if since_id and stop_on_id:
            page = [item for item in page if int(item["id"]) > int(since_id)]
            if not page:
                break
        all_items.extend(page)
        pages_fetched += 1

        if pages_fetched % 10 == 0:
            logger.info(f"  ...fetched {len(all_items)} items so far ({pages_fetched} pages)")

        time.sleep(1.0)  # Be gentle on the instance between pages

    return all_items, new_cursor


def sync_toots(client: Mastodon):
    """Sync user's own statuses."""
    logger.info("Syncing toots...")
    me = client.me()
    account_id = me["id"]

    with get_db() as conn:
        since_id = get_sync_state(conn, "toots_since_id")

    def fetch(**kwargs):
        return client.account_statuses(account_id, **kwargs)

    statuses, _ = _fetch_all_pages(fetch, client=client, since_id=since_id)
    if not statuses:
        logger.info("No new toots found.")
        return 0

    with get_db() as conn:
        for status in statuses:
            upsert_toot(conn, status)
            download_media(status)
        newest_id = str(max(int(s["id"]) for s in statuses))
        if not since_id or int(newest_id) > int(since_id):
            set_sync_state(conn, "toots_since_id", newest_id)

    logger.info(f"Synced {len(statuses)} toots.")
    return len(statuses)


def sync_notification_requests(client: Mastodon):
    """Sync filtered notification requests (mentions from non-followers on Mastodon 4.3+).

    These live in a separate inbox and are not returned by the standard
    notifications endpoint, so we fetch them separately and upsert them
    into the same notifications table.
    """
    logger.info("Syncing notification requests...")
    base_url = client.api_base_url.rstrip("/")
    headers = {"Authorization": f"Bearer {client.access_token}"}

    # Fetch the list of pending notification requests
    try:
        resp = requests.get(
            f"{base_url}/api/v1/notifications/requests",
            headers=headers,
            params={"limit": 80},
            timeout=15,
        )
        if resp.status_code in (404, 501):
            logger.info("Notification requests not supported by this server — skipping.")
            return 0
        resp.raise_for_status()
        req_list = resp.json()
    except Exception as e:
        logger.warning(f"Notification requests fetch failed: {e}")
        return 0

    if not req_list:
        return 0

    count = 0
    for req in req_list:
        req_id = req.get("id")
        if not req_id:
            continue
        try:
            notif_resp = requests.get(
                f"{base_url}/api/v1/notifications/requests/{req_id}/notifications",
                headers=headers,
                params={"limit": 80},
                timeout=15,
            )
            if notif_resp.status_code == 404:
                continue
            notif_resp.raise_for_status()
            notifs = notif_resp.json()
            with get_db() as conn:
                for notif in notifs:
                    upsert_notification(conn, notif)
                    count += 1
        except Exception as e:
            logger.warning(f"Failed fetching notifications for request {req_id}: {e}")

    logger.info(f"Synced {count} notifications from filtered requests.")
    return count


def sync_notifications(client: Mastodon):
    """Sync notifications (likes, boosts, replies on user's toots)."""
    logger.info("Syncing notifications...")

    with get_db() as conn:
        since_id = get_sync_state(conn, "notifications_since_id")

    def fetch(**kwargs):
        return client.notifications(**kwargs)

    notifs, _ = _fetch_all_pages(fetch, client=client, since_id=since_id)
    if not notifs:
        logger.info("No new notifications found.")
        return 0

    with get_db() as conn:
        for notif in notifs:
            upsert_notification(conn, notif)
        newest_id = str(max(int(n["id"]) for n in notifs))
        if not since_id or int(newest_id) > int(since_id):
            set_sync_state(conn, "notifications_since_id", newest_id)

    logger.info(f"Synced {len(notifs)} notifications.")
    return len(notifs)


def sync_favorites(client: Mastodon):
    """Sync toots the user has favorited."""
    logger.info("Syncing favorites...")

    with get_db() as conn:
        cursor = get_sync_state(conn, "favorites_cursor")

    def fetch(**kwargs):
        if cursor:
            kwargs["min_id"] = cursor
        return client.favourites(**kwargs)

    favs, _ = _fetch_all_pages(fetch, client=client, stop_on_id=False)
    if not favs:
        logger.info("No new favorites found.")
        return 0

    with get_db() as conn:
        for fav in favs:
            upsert_favorite(conn, fav)
            download_media(fav)
        newest_id = str(max(int(f["id"]) for f in favs))
        set_sync_state(conn, "favorites_cursor", newest_id)

    logger.info(f"Synced {len(favs)} favorites.")
    return len(favs)


def sync_bookmarks(client: Mastodon):
    """Sync toots the user has bookmarked."""
    logger.info("Syncing bookmarks...")

    with get_db() as conn:
        cursor = get_sync_state(conn, "bookmarks_cursor")

    def fetch(**kwargs):
        if cursor:
            kwargs["min_id"] = cursor
        return client.bookmarks(**kwargs)

    bmarks, _ = _fetch_all_pages(fetch, client=client, stop_on_id=False)
    if not bmarks:
        logger.info("No new bookmarks found.")
        return 0

    with get_db() as conn:
        for bm in bmarks:
            upsert_bookmark(conn, bm)
            download_media(bm)
        newest_id = str(max(int(b["id"]) for b in bmarks))
        set_sync_state(conn, "bookmarks_cursor", newest_id)

    logger.info(f"Synced {len(bmarks)} bookmarks.")
    return len(bmarks)


def sync_followers(client: Mastodon):
    """Sync followers, recording follow/unfollow events."""
    logger.info("Syncing followers...")
    me = client.me()
    account_id = me["id"]

    # Fetch all current followers (paginated)
    current_followers: dict[str, dict] = {}
    page = client.account_followers(account_id, limit=80)
    while page:
        for acc in page:
            current_followers[str(acc["id"])] = acc
        page = client.fetch_next(page) if hasattr(page, "_pagination_next") and page._pagination_next else None

    with get_db() as conn:
        stored = conn.execute("SELECT account_id, acct FROM followers").fetchall()
        stored_ids = {row["account_id"] for row in stored}
        current_ids = set(current_followers.keys())
        is_first_run = len(stored_ids) == 0

        now = datetime.now(timezone.utc).isoformat()

        # New followers
        for acc_id in current_ids - stored_ids:
            acc = current_followers[acc_id]
            avatar = acc.get("avatar", "")
            acct = acc.get("acct", "")
            display_name = acc.get("display_name", "") or acct
            conn.execute(
                "INSERT INTO followers (account_id, acct, display_name, avatar, followed_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(account_id) DO UPDATE SET "
                "acct=excluded.acct, display_name=excluded.display_name, avatar=excluded.avatar, updated_at=excluded.updated_at",
                (acc_id, acct, display_name, avatar, now, now),
            )
            if not is_first_run:
                conn.execute(
                    "INSERT INTO follower_events (event_type, account_id, acct, display_name, avatar, occurred_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    ("followed", acc_id, acct, display_name, avatar, now),
                )

        # Unfollowers
        for acc_id in stored_ids - current_ids:
            row = conn.execute("SELECT * FROM followers WHERE account_id=?", (acc_id,)).fetchone()
            if row and not is_first_run:
                conn.execute(
                    "INSERT INTO follower_events (event_type, account_id, acct, display_name, avatar, occurred_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    ("unfollowed", acc_id, row["acct"], row["display_name"], row["avatar"], now),
                )
            conn.execute("DELETE FROM followers WHERE account_id=?", (acc_id,))

    new_count = len(current_ids - stored_ids) if not is_first_run else 0
    lost_count = len(stored_ids - current_ids) if not is_first_run else 0
    logger.info(f"Followers synced. +{new_count} followed, -{lost_count} unfollowed. Total: {len(current_ids)}")
    return len(current_ids)


def run_full_sync():
    """Run a complete sync of all data types."""
    logger.info("Starting full sync...")
    try:
        client = get_client()
        counts = {
            "toots": sync_toots(client),
            "notifications": sync_notifications(client) + sync_notification_requests(client),
            "favorites": sync_favorites(client),
            "bookmarks": sync_bookmarks(client),
            "followers": sync_followers(client),
        }
        # Export new toots to Markdown backup
        try:
            from app.markdown_export import export_new_toots
            with get_db() as conn:
                counts["markdown"] = export_new_toots(conn)
        except Exception:
            logger.exception("Markdown export failed (non-fatal)")
        logger.info(f"Full sync complete: {counts}")
        return counts
    except Exception:
        logger.exception("Error during sync")
        raise
