import logging
import math
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from mastodon import Mastodon

from app.collector import run_full_sync
from app.config import APP_URL, MASTODON_ACCESS_TOKEN, MASTODON_INSTANCE, MEDIA_PATH, POLL_INTERVAL
from app.database import (
    get_all_settings,
    get_bookmarks,
    get_db,
    get_favorites,
    generate_roast,
    get_hashtag_counts,
    get_notifications,
    get_setting,
    get_stats,
    get_toot_detail,
    get_toots,
    get_topic_counts,
    init_db,
    is_configured,
    set_setting,
)
from app.search import search

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

APP_DIR = Path(__file__).parent
OAUTH_SCOPES = "read"

scheduler = BackgroundScheduler()
sync_lock = threading.Lock()


def _get_credentials() -> tuple[str, str] | None:
    """Get instance URL and access token from DB, falling back to env vars."""
    with get_db() as conn:
        instance = get_setting(conn, "instance_url") or MASTODON_INSTANCE
        token = get_setting(conn, "access_token") or MASTODON_ACCESS_TOKEN
    if instance and token:
        return instance, token
    return None


def _run_sync_job():
    if not sync_lock.acquire(blocking=False):
        logger.info("Sync already running, skipping.")
        return
    try:
        run_full_sync()
    except Exception:
        logger.exception("Scheduled sync failed")
    finally:
        sync_lock.release()


def _start_scheduler():
    """Start the sync scheduler if credentials are available."""
    creds = _get_credentials()
    if not creds:
        logger.warning("No credentials configured. Syncing disabled.")
        return

    # Run initial sync in background thread
    threading.Thread(target=_run_sync_job, daemon=True).start()

    if not scheduler.running:
        try:
            scheduler.add_job(_run_sync_job, "interval", minutes=POLL_INTERVAL, id="sync_job", replace_existing=True)
            scheduler.start()
            logger.info("Scheduler started.")
        except Exception:
            logger.exception("Failed to start scheduler")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    # Migrate env vars to DB if DB has no credentials but env vars are set
    with get_db() as conn:
        if not is_configured(conn) and MASTODON_INSTANCE and MASTODON_ACCESS_TOKEN:
            set_setting(conn, "instance_url", MASTODON_INSTANCE)
            set_setting(conn, "access_token", MASTODON_ACCESS_TOKEN)
            logger.info("Migrated credentials from env vars to database.")

    logger.info(f"Poll interval: {POLL_INTERVAL} minutes")
    _start_scheduler()

    yield

    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Tootkeeper", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")

# Serve downloaded media files
media_dir = Path(MEDIA_PATH)
media_dir.mkdir(parents=True, exist_ok=True)
app.mount("/media", StaticFiles(directory=str(media_dir)), name="media")

templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


def _media_url(attachment: dict) -> str | None:
    """Return a local /media/ URL if the file was downloaded, else the remote URL."""
    import json as _json
    import os as _os
    from urllib.parse import urlparse as _urlparse

    media_id = str(attachment.get("id", ""))
    if not media_id:
        return attachment.get("url") or attachment.get("preview_url")

    # Check for local file
    for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4"):
        local = media_dir / f"{media_id}{ext}"
        if local.exists():
            return f"/media/{media_id}{ext}"

    # Fall back to remote
    return attachment.get("url") or attachment.get("preview_url")


def _media_preview_url(attachment: dict) -> str | None:
    """Return local preview URL if available, else remote preview."""
    media_id = str(attachment.get("id", ""))
    if not media_id:
        return attachment.get("preview_url")

    for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
        local = media_dir / f"{media_id}_preview{ext}"
        if local.exists():
            return f"/media/{media_id}_preview{ext}"

    return attachment.get("preview_url")


# Register as Jinja2 globals/filters so templates can use them
templates.env.globals["media_url"] = _media_url
templates.env.globals["media_preview_url"] = _media_preview_url

import json as _json
templates.env.filters["fromjson"] = lambda s: _json.loads(s) if isinstance(s, str) else s


def _paginate(page: int, per_page: int, total: int) -> dict:
    total_pages = max(1, math.ceil(total / per_page))
    return {
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }


def _require_setup(request: Request) -> RedirectResponse | None:
    """Return a redirect to /setup if not configured, else None."""
    with get_db() as conn:
        if not is_configured(conn):
            return RedirectResponse(url="/setup", status_code=302)
    return None


# ─── OAuth / Setup ────────────────────────────────────────────────

@app.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request, error: str = ""):
    with get_db() as conn:
        settings = get_all_settings(conn)
    return templates.TemplateResponse("setup.html", {
        "request": request,
        "settings": settings,
        "error": error,
    })


@app.post("/auth/login")
async def auth_login(request: Request):
    """Register app with the Mastodon instance and redirect to authorize."""
    form = await request.form()
    instance_url = str(form.get("instance_url", "")).strip().rstrip("/")

    if not instance_url:
        return RedirectResponse(url="/setup?error=Please+enter+your+instance+URL", status_code=302)

    # Ensure it has a scheme
    if not instance_url.startswith("http"):
        instance_url = "https://" + instance_url

    redirect_uri = APP_URL.rstrip("/") + "/auth/callback"

    try:
        # Register the app with the instance
        client_id, client_secret = Mastodon.create_app(
            "Tootkeeper",
            scopes=OAUTH_SCOPES.split(),
            redirect_uris=redirect_uri,
            api_base_url=instance_url,
        )

        # Store credentials in DB
        with get_db() as conn:
            set_setting(conn, "instance_url", instance_url)
            set_setting(conn, "client_id", client_id)
            set_setting(conn, "client_secret", client_secret)
            set_setting(conn, "redirect_uri", redirect_uri)

        # Create a client to get the auth URL
        client = Mastodon(
            client_id=client_id,
            client_secret=client_secret,
            api_base_url=instance_url,
        )
        auth_url = client.auth_request_url(
            scopes=OAUTH_SCOPES.split(),
            redirect_uris=redirect_uri,
        )

        return RedirectResponse(url=auth_url, status_code=302)

    except Exception as e:
        logger.exception("Failed to register app with instance")
        error_msg = f"Could not connect to {instance_url}: {e}"
        return RedirectResponse(
            url=f"/setup?error={error_msg}",
            status_code=302,
        )


@app.get("/auth/callback")
async def auth_callback(request: Request, code: str = ""):
    """Handle the OAuth callback from the Mastodon instance."""
    if not code:
        return RedirectResponse(url="/setup?error=Authorization+was+denied+or+failed", status_code=302)

    with get_db() as conn:
        instance_url = get_setting(conn, "instance_url")
        client_id = get_setting(conn, "client_id")
        client_secret = get_setting(conn, "client_secret")
        redirect_uri = get_setting(conn, "redirect_uri")

    if not all([instance_url, client_id, client_secret]):
        return RedirectResponse(url="/setup?error=Missing+app+credentials.+Please+try+again.", status_code=302)

    try:
        client = Mastodon(
            client_id=client_id,
            client_secret=client_secret,
            api_base_url=instance_url,
        )
        access_token = client.log_in(
            code=code,
            redirect_uri=redirect_uri,
            scopes=OAUTH_SCOPES.split(),
        )

        # Verify the token works
        client.access_token = access_token
        me = client.me()

        with get_db() as conn:
            set_setting(conn, "access_token", access_token)
            set_setting(conn, "account_id", str(me["id"]))
            set_setting(conn, "account_acct", me["acct"])
            set_setting(conn, "account_display_name", me.get("display_name", ""))
            set_setting(conn, "account_avatar", me.get("avatar", ""))

        logger.info(f"Successfully authenticated as @{me['acct']}@{instance_url}")

        # Start syncing now that we have credentials
        _start_scheduler()

        return RedirectResponse(url="/", status_code=302)

    except Exception as e:
        logger.exception("OAuth callback failed")
        return RedirectResponse(
            url=f"/setup?error=Login+failed:+{e}",
            status_code=302,
        )


@app.get("/auth/logout")
async def auth_logout():
    """Clear stored credentials and stop syncing."""
    if scheduler.running:
        scheduler.shutdown(wait=False)

    with get_db() as conn:
        for key in ["access_token", "client_id", "client_secret", "redirect_uri",
                     "account_id", "account_acct", "account_display_name", "account_avatar"]:
            conn.execute("DELETE FROM app_settings WHERE key=?", (key,))

    return RedirectResponse(url="/setup", status_code=302)


@app.post("/settings/ai")
async def save_ai_settings(request: Request):
    """Save AI provider settings."""
    form = await request.form()
    with get_db() as conn:
        for key in ("ai_provider", "ai_api_key", "ai_model", "ai_base_url"):
            value = str(form.get(key, "")).strip()
            set_setting(conn, key, value)
        # Clear cached roast data so next dashboard load generates fresh
        conn.execute("DELETE FROM app_settings WHERE key IN ('roast_current', 'roast_pool')")
    return RedirectResponse(url="/settings?saved=1", status_code=302)


@app.post("/api/roast")
async def api_regenerate_roast():
    """Force-regenerate the AI roast."""
    with get_db() as conn:
        roast = generate_roast(conn, force=True)
    if not roast:
        return JSONResponse({"status": "error", "message": "AI not configured or API call failed"}, status_code=400)
    return JSONResponse({"status": "ok", "roast": roast})


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, saved: str = ""):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        settings = get_all_settings(conn)
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "settings": settings,
        "saved": bool(saved),
        "poll_interval": POLL_INTERVAL,
    })


# ─── Main pages ───────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        stats = get_stats(conn)
        toots, _ = get_toots(conn, page=1, per_page=10)
        notifs, _ = get_notifications(conn, page=1, per_page=10)
        settings = get_all_settings(conn)
        roast = generate_roast(conn)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "stats": stats,
        "recent_toots": toots,
        "recent_notifications": notifs,
        "account": settings,
        "roast": roast,
    })


@app.get("/toots", response_class=HTMLResponse)
async def toots_page(request: Request, page: int = Query(1, ge=1)):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        items, total = get_toots(conn, page=page)
    pagination = _paginate(page, 20, total)
    return templates.TemplateResponse("toots.html", {
        "request": request,
        "items": items,
        "pagination": pagination,
    })


@app.get("/notifications", response_class=HTMLResponse)
async def notifications_page(
    request: Request,
    page: int = Query(1, ge=1),
    type: str = Query("", alias="type"),
):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        items, total = get_notifications(conn, page=page, type_filter=type)
    pagination = _paginate(page, 20, total)
    return templates.TemplateResponse("notifications.html", {
        "request": request,
        "items": items,
        "pagination": pagination,
        "type_filter": type,
    })


@app.get("/favorites", response_class=HTMLResponse)
async def favorites_page(request: Request, page: int = Query(1, ge=1)):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        items, total = get_favorites(conn, page=page)
    pagination = _paginate(page, 20, total)
    return templates.TemplateResponse("favorites.html", {
        "request": request,
        "items": items,
        "pagination": pagination,
    })


@app.get("/bookmarks", response_class=HTMLResponse)
async def bookmarks_page(request: Request, page: int = Query(1, ge=1)):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        items, total = get_bookmarks(conn, page=page)
    pagination = _paginate(page, 20, total)
    return templates.TemplateResponse("bookmarks.html", {
        "request": request,
        "items": items,
        "pagination": pagination,
    })


@app.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = Query(""),
    type: str = Query(""),
    page: int = Query(1, ge=1),
):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    results = []
    total = 0
    if q:
        with get_db() as conn:
            results, total = search(conn, q, source_type=type, page=page)
    pagination = _paginate(page, 20, total)
    return templates.TemplateResponse("search.html", {
        "request": request,
        "query": q,
        "type_filter": type,
        "results": results,
        "pagination": pagination,
    })


@app.get("/hashtags", response_class=HTMLResponse)
async def hashtags_page(request: Request):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        hashtags = get_hashtag_counts(conn)
    return templates.TemplateResponse("hashtags.html", {
        "request": request,
        "hashtags": hashtags,
        "total": len(hashtags),
    })


@app.get("/topics", response_class=HTMLResponse)
async def topics_page(request: Request):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        topics = get_topic_counts(conn)
    return templates.TemplateResponse("topics.html", {
        "request": request,
        "topics": topics,
        "total": len(topics),
    })


@app.get("/toot/{toot_id}", response_class=HTMLResponse)
async def toot_detail(request: Request, toot_id: str):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        toot = get_toot_detail(conn, toot_id)
    if not toot:
        return HTMLResponse("<h1>Toot not found</h1>", status_code=404)
    return templates.TemplateResponse("detail.html", {
        "request": request,
        "toot": toot,
    })


@app.get("/api/stats")
async def api_stats():
    with get_db() as conn:
        stats = get_stats(conn)
    return JSONResponse(stats)


@app.post("/api/sync")
async def api_sync():
    if sync_lock.locked():
        return JSONResponse({"status": "already_running"})
    creds = _get_credentials()
    if not creds:
        return JSONResponse({"status": "not_configured"}, status_code=400)
    threading.Thread(target=_run_sync_job, daemon=True).start()
    return JSONResponse({"status": "started"})
