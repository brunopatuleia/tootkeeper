import logging
import os
import time
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
    )


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
        if media_type not in ("image", "gifv"):
            continue

        url = media.get("url") or media.get("remote_url")
        preview_url = media.get("preview_url")

        if url:
            ext = _get_extension(url)
            local_path = Path(MEDIA_PATH) / f"{media_id}{ext}"
            if not local_path.exists():
                _download_file(url, local_path)

        if preview_url:
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

        time.sleep(0.3)  # Rate limiting

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

    favs, new_cursor = _fetch_all_pages(fetch, client=None, stop_on_id=False)
    if not favs:
        logger.info("No new favorites found.")
        return 0

    with get_db() as conn:
        for fav in favs:
            upsert_favorite(conn, fav)
            download_media(fav)
        
        if new_cursor:
            set_sync_state(conn, "favorites_cursor", new_cursor)

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

    bmarks, new_cursor = _fetch_all_pages(fetch, client=None, stop_on_id=False)
    if not bmarks:
        logger.info("No new bookmarks found.")
        return 0

    with get_db() as conn:
        for bm in bmarks:
            upsert_bookmark(conn, bm)
            download_media(bm)
        
        if new_cursor:
            set_sync_state(conn, "bookmarks_cursor", new_cursor)

    logger.info(f"Synced {len(bmarks)} bookmarks.")
    return len(bmarks)


def run_full_sync():
    """Run a complete sync of all data types."""
    logger.info("Starting full sync...")
    try:
        client = get_client()
        counts = {
            "toots": sync_toots(client),
            "notifications": sync_notifications(client),
            "favorites": sync_favorites(client),
            "bookmarks": sync_bookmarks(client),
        }
        logger.info(f"Full sync complete: {counts}")
        return counts
    except Exception:
        logger.exception("Error during sync")
        raise
