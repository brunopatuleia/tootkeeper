import hashlib
import hmac
import logging
import math
import secrets
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote as _url_quote

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from mastodon import Mastodon

from app.collector import run_full_sync
from app.config import APP_PASSWORD, APP_URL, GITHUB_REPO, MASTODON_ACCESS_TOKEN, MASTODON_INSTANCE, MEDIA_PATH, POLL_INTERVAL, VERSION
from app.profile_updater import ProfileUpdater
from app.roast import generate_roast, _add_to_roast_history
from app.database import (
    get_all_settings,
    get_bookmarks,
    get_db,
    get_favorites,
    get_follower_chart_data,
    get_follower_counts,
    get_follower_events,
    get_unfollowers,
    get_hashtag_counts,
    get_notifications,
    get_setting,
    get_stats,
    get_toot_detail,
    get_toots,
    get_topic_counts,
    get_top_repliers,
    get_top_replied_to,
    init_db,
    is_configured,
    set_setting,
)
from app.search import search
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

APP_DIR = Path(__file__).parent
OAUTH_SCOPES = "read write:accounts write:statuses write:media"

scheduler = BackgroundScheduler()
sync_lock = threading.Lock()
profile_updater = ProfileUpdater()

_ROAST_COOLDOWN_SECONDS = 30
_last_roast_request: float = 0
_roast_lock = threading.Lock()

_version_cache: dict = {"latest": None, "ts": 0.0}

# Auth — populated during lifespan startup
_secret_key: str = ""
_AUTH_COOKIE = "tk_auth"


def _auth_token() -> str:
    return hashlib.sha256(f"{APP_PASSWORD}:{_secret_key}".encode()).hexdigest()


def _is_authenticated(request: Request) -> bool:
    if not APP_PASSWORD:
        return True
    return request.cookies.get(_AUTH_COOKIE) == _auth_token()


import socket as _socket

_BLOCKED_HOSTS = {"169.254.169.254", "169.254.170.2", "metadata.google.internal"}

def _safe_url(url: str) -> bool:
    """Return False for cloud metadata endpoints and loopback addresses. Private IPs allowed (homelab)."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        hostname = (parsed.hostname or "").lower()
        if hostname in _BLOCKED_HOSTS:
            return False
        try:
            ip = _socket.gethostbyname(hostname)
            if ip.startswith("127.") or ip in ("0.0.0.0", "::1"):
                return False
        except _socket.gaierror:
            return False
        return True
    except Exception:
        return False


def _safe_next(url: str) -> str:
    """Allow only relative paths to prevent open redirect attacks."""
    if url and url.startswith("/") and not url.startswith("//"):
        return url
    return "/"


def _require_auth(request: Request) -> "RedirectResponse | None":
    if not _is_authenticated(request):
        next_path = _url_quote(request.url.path)
        return RedirectResponse(url=f"/login?next={next_path}", status_code=302)
    return None


def _require_auth_api(request: Request) -> "JSONResponse | None":
    if not _is_authenticated(request):
        return JSONResponse({"status": "error", "message": "Unauthorized"}, status_code=401)
    return None


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
    global _secret_key
    init_db()

    # Generate and persist a secret key used to sign auth cookies
    with get_db() as conn:
        _secret_key = get_setting(conn, "secret_key") or ""
        if not _secret_key:
            _secret_key = secrets.token_hex(32)
            set_setting(conn, "secret_key", _secret_key)

    # Migrate env vars to DB if DB has no credentials but env vars are set
    with get_db() as conn:
        if not is_configured(conn) and MASTODON_INSTANCE and MASTODON_ACCESS_TOKEN:
            set_setting(conn, "instance_url", MASTODON_INSTANCE)
            set_setting(conn, "access_token", MASTODON_ACCESS_TOKEN)
            logger.info("Migrated credentials from env vars to database.")

    logger.info(f"Poll interval: {POLL_INTERVAL} minutes")
    _start_scheduler()

    # Start profile updater if enabled (profile fields, ABS, or both)
    with get_db() as conn:
        pu_on = get_setting(conn, "pu_enabled") == "1"
        abs_on = get_setting(conn, "pu_abs_enabled") == "1"
        if (pu_on or abs_on) and is_configured(conn):
            profile_updater.start()

    yield

    profile_updater.stop()
    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Mastoferr", lifespan=lifespan)
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
templates.env.globals["APP_VERSION"] = VERSION


def _get_app_settings() -> dict:
    try:
        with get_db() as conn:
            return {"interactions_tab_name": get_setting(conn, "interactions_tab_name") or ""}
    except Exception:
        return {}


templates.env.globals["app_settings"] = _get_app_settings

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
    """Return a redirect to /login or /setup if not authorized/configured, else None."""
    auth = _require_auth(request)
    if auth:
        return auth
    with get_db() as conn:
        if not is_configured(conn):
            return RedirectResponse(url="/setup", status_code=302)
    return None


# ─── Auth ─────────────────────────────────────────────────────────

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/", error: str = ""):
    if _is_authenticated(request):
        return RedirectResponse(url=_safe_next(next), status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "next": next, "error": error})


@app.post("/login")
async def login_submit(request: Request):
    form = await request.form()
    password = str(form.get("password", ""))
    next_url = _safe_next(str(form.get("next", "/")))
    if APP_PASSWORD and hmac.compare_digest(password, APP_PASSWORD):
        response = RedirectResponse(url=next_url, status_code=302)
        response.set_cookie(_AUTH_COOKIE, _auth_token(), httponly=True, samesite="strict")
        return response
    return templates.TemplateResponse("login.html", {
        "request": request, "next": next_url, "error": "Incorrect password",
    }, status_code=401)


@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(_AUTH_COOKIE)
    return response


# ─── OAuth / Setup ────────────────────────────────────────────────

@app.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request, error: str = ""):
    auth = _require_auth(request)
    if auth:
        return auth
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
    auth = _require_auth(request)
    if auth:
        return auth
    form = await request.form()
    instance_url = str(form.get("instance_url", "")).strip().rstrip("/")

    if not instance_url:
        return RedirectResponse(url="/setup?error=Please+enter+your+instance+URL", status_code=302)

    # Ensure it has a scheme
    if not instance_url.startswith("http"):
        instance_url = "https://" + instance_url

    if not _safe_url(instance_url):
        return RedirectResponse(url="/setup?error=Invalid+instance+URL", status_code=302)

    redirect_uri = APP_URL.rstrip("/") + "/auth/callback"

    # Generate OAuth state token to prevent CSRF on the callback
    oauth_state = secrets.token_hex(16)

    try:
        # Register the app with the instance
        client_id, client_secret = Mastodon.create_app(
            "Mastoferr",
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
        # Append state parameter manually (Mastodon.py doesn't expose it)
        separator = "&" if "?" in auth_url else "?"
        auth_url = f"{auth_url}{separator}state={oauth_state}"

        response = RedirectResponse(url=auth_url, status_code=302)
        response.set_cookie("oauth_state", oauth_state, httponly=True, samesite="lax", max_age=600)
        return response

    except Exception as e:
        logger.exception("Failed to register app with instance")
        error_msg = _url_quote(f"Could not connect to {instance_url}: {e}")
        return RedirectResponse(
            url=f"/setup?error={error_msg}",
            status_code=302,
        )


@app.get("/auth/callback")
async def auth_callback(request: Request, code: str = "", state: str = ""):
    """Handle the OAuth callback from the Mastodon instance."""
    if not code:
        return RedirectResponse(url="/setup?error=Authorization+was+denied+or+failed", status_code=302)

    # Verify OAuth state to prevent CSRF on the callback
    expected_state = request.cookies.get("oauth_state", "")
    if not expected_state or not hmac.compare_digest(state, expected_state):
        return RedirectResponse(url="/setup?error=Invalid+OAuth+state.+Please+try+again.", status_code=302)

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

        response = RedirectResponse(url="/", status_code=302)
        response.delete_cookie("oauth_state")
        return response

    except Exception as e:
        logger.exception("OAuth callback failed")
        return RedirectResponse(
            url=f"/setup?error={_url_quote(f'Login failed: {e}')}",
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
    auth = _require_auth(request)
    if auth:
        return auth
    form = await request.form()
    with get_db() as conn:
        for key in ("ai_provider", "ai_api_key", "ai_model", "ai_base_url"):
            value = str(form.get(key, "")).strip()
            if not value and key == "ai_api_key":
                continue  # Never overwrite a stored API key with empty
            set_setting(conn, key, value)
        # Clear cached roast data so next dashboard load generates fresh
        conn.execute("DELETE FROM app_settings WHERE key IN ('roast_current', 'roast_pool')")
    return RedirectResponse(url="/settings?saved=1#ai-roast", status_code=302)


@app.post("/api/roast")
async def api_regenerate_roast(request: Request):
    """Force-regenerate the AI roast."""
    auth = _require_auth_api(request)
    if auth:
        return auth
    global _last_roast_request
    with _roast_lock:
        now = time.time()
        if now - _last_roast_request < _ROAST_COOLDOWN_SECONDS:
            remaining = int(_ROAST_COOLDOWN_SECONDS - (now - _last_roast_request))
            return JSONResponse(
                {"status": "error", "message": f"Please wait {remaining}s before generating another roast"},
                status_code=429,
            )
        _last_roast_request = now
    with get_db() as conn:
        roast = generate_roast(conn, force=True)
    if not roast:
        return JSONResponse({"status": "error", "message": "AI not configured or API call failed"}, status_code=400)

    return JSONResponse({"status": "ok", "roast": roast})


@app.post("/api/roast/toot")
async def api_toot_roast(request: Request):
    """Post the current roast to Mastodon."""
    auth = _require_auth_api(request)
    if auth:
        return auth
    with get_db() as conn:
        roast = get_setting(conn, "roast_current")
    if not roast:
        return JSONResponse({"status": "error", "message": "No roast to post"}, status_code=400)
    creds = _get_credentials()
    if not creds:
        return JSONResponse({"status": "error", "message": "Mastodon not configured"}, status_code=400)
    try:
        instance_url, access_token = creds
        client = Mastodon(access_token=access_token, api_base_url=instance_url)
        client.status_post(roast, visibility="public")
        return JSONResponse({"status": "ok"})
    except Exception:
        logger.exception("Failed to toot the roast")
        return JSONResponse({"status": "error", "message": "Failed to post to Mastodon"}, status_code=500)


@app.post("/api/roast/rate")
async def api_rate_roast(request: Request):
    """Save a like (1) or dislike (-1) rating for the current roast."""
    auth = _require_auth_api(request)
    if auth:
        return auth
    body = await request.json()
    rating = body.get("rating")
    if rating not in (1, -1):
        return JSONResponse({"status": "error", "message": "rating must be 1 or -1"}, status_code=400)
    with get_db() as conn:
        roast = get_setting(conn, "roast_current")
        if not roast:
            return JSONResponse({"status": "error", "message": "No active roast"}, status_code=400)
        conn.execute(
            "INSERT INTO roast_ratings (roast_text, rating) VALUES (?, ?)",
            (roast, rating),
        )
        if rating == -1:
            # Add to history so this roast is never served again
            _add_to_roast_history(conn, roast)
            # Also remove it from the pool if it's still there
            pool_raw = get_setting(conn, "roast_pool")
            if pool_raw:
                import json as _json
                pool = _json.loads(pool_raw)
                pool = [r for r in pool if r != roast]
                set_setting(conn, "roast_pool", _json.dumps(pool))
    return JSONResponse({"status": "ok"})


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
        "version": VERSION,
        "pu_status": profile_updater.get_status(),
    })


@app.get("/api/version")
async def api_version():
    """Check for updates by comparing local version with latest GitHub tag."""
    current = VERSION
    now = time.time()

    # Return cached result if less than 1 hour old
    if _version_cache["latest"] and (now - _version_cache["ts"] < 3600):
        latest = _version_cache["latest"]
        return JSONResponse({
            "current": current,
            "latest": latest,
            "update_available": latest != current,
        })

    latest = None
    update_available = False
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/tags?per_page=1"
        response = requests.get(url, headers={"User-Agent": "Mastoferr"}, timeout=5)
        response.raise_for_status()
        tags = response.json()
        if tags:
            latest = tags[0]["name"].lstrip("v")
            update_available = latest != current
            _version_cache["latest"] = latest
            _version_cache["ts"] = now
    except (requests.RequestException, KeyError, IndexError):
        logger.debug("Failed to check for updates on GitHub")

    return JSONResponse({
        "current": current,
        "latest": latest,
        "update_available": update_available,
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
        "instance_url": settings.get("instance_url", ""),
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


@app.get("/followers", response_class=HTMLResponse)
async def followers_page(request: Request, page: int = Query(1, ge=1)):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        events, total = get_follower_events(conn, page=page)
        counts = get_follower_counts(conn)
        chart = get_follower_chart_data(conn)
        unfollowers = get_unfollowers(conn)
    pagination = _paginate(page, 40, total)
    return templates.TemplateResponse("followers.html", {
        "request": request,
        "events": events,
        "counts": counts,
        "chart": chart,
        "unfollowers": unfollowers,
        "pagination": pagination,
    })


@app.get("/interactions", response_class=HTMLResponse)
async def interactions_page(request: Request):
    redirect = _require_setup(request)
    if redirect:
        return redirect
    with get_db() as conn:
        days = max(1, int(get_setting(conn, "interactions_days") or 15))
        repliers = get_top_repliers(conn, days=days)
        replied_to = get_top_replied_to(conn, days=days)
        tab_name = get_setting(conn, "interactions_tab_name") or "Friends or Stalkers"
    return templates.TemplateResponse("interactions.html", {
        "request": request,
        "repliers": repliers,
        "replied_to": replied_to,
        "tab_name": tab_name,
        "days": days,
    })


@app.post("/settings/app")
async def settings_app(request: Request):
    if (auth := _require_auth(request)):
        return auth
    form = await request.form()
    with get_db() as conn:
        set_setting(conn, "interactions_tab_name", str(form.get("interactions_tab_name", "")).strip())
        days_val = str(form.get("interactions_days", "")).strip()
        if days_val.isdigit() and int(days_val) >= 1:
            set_setting(conn, "interactions_days", days_val)
    return RedirectResponse(url="/settings?saved=1#app", status_code=302)


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


@app.get("/health")
async def health_check():
    """Lightweight liveness probe — no auth required."""
    return JSONResponse({"status": "ok"})


@app.get("/api/stats")
async def api_stats(request: Request):
    if (auth := _require_auth_api(request)):
        return auth
    with get_db() as conn:
        stats = get_stats(conn)
    return JSONResponse(stats)


@app.post("/api/sync")
async def api_sync(request: Request):
    if (auth := _require_auth_api(request)):
        return auth
    if sync_lock.locked():
        return JSONResponse({"status": "already_running"})
    creds = _get_credentials()
    if not creds:
        return JSONResponse({"status": "not_configured"}, status_code=400)
    threading.Thread(target=_run_sync_job, daemon=True).start()
    return JSONResponse({"status": "started"})


# ─── Profile Updater ─────────────────────────────────────────────

SERVICES_SETTINGS_KEYS = [
    "pu_lastfm_username", "pu_lastfm_api_key",
    "pu_listenbrainz_username", "pu_listenbrainz_token",
    "pu_navidrome_url", "pu_navidrome_username", "pu_navidrome_password",
    "pu_spotify_client_id", "pu_spotify_client_secret", "pu_spotify_refresh_token",
    "pu_jellyfin_url", "pu_jellyfin_api_key", "pu_jellyfin_user_id",
    "pu_plex_url", "pu_plex_token",
    "pu_tautulli_url", "pu_tautulli_api_key",
    "pu_letterboxd_rss_url",
    "pu_goodreads_rss_url",
    "pu_abs_url", "pu_abs_token",
]

# Secret fields are never sent back to the browser. On save, only overwrite if non-empty.
SERVICES_SECRET_KEYS = {
    "pu_lastfm_api_key", "pu_listenbrainz_token", "pu_navidrome_password",
    "pu_spotify_client_secret", "pu_spotify_refresh_token",
    "pu_jellyfin_api_key", "pu_plex_token", "pu_tautulli_api_key", "pu_abs_token",
}

PU_SETTINGS_KEYS = [
    "pu_music_field_name", "pu_movie_field_name", "pu_book_field_name",
    "pu_music_interval", "pu_movie_interval", "pu_book_interval",
    "pu_custom_field_name", "pu_custom_field_value",
    "pu_field_order",
]

PU_CHECKBOX_KEYS = [
    "pu_music_enabled", "pu_movies_enabled", "pu_books_enabled",
    "pu_custom_enabled", "pu_show_emoji",
]

AUTO_TOOTS_SETTINGS_KEYS = [
    "pu_weekly_artists_hashtags", "pu_weekly_artists_day", "pu_weekly_artists_hour",
    "pu_books_hashtags",
    "pu_album_hashtags",
    "pu_abs_hashtags", "pu_abs_interval",
    "pu_abs_finished_hashtags",
]

AUTO_TOOTS_CHECKBOX_KEYS = [
    "pu_weekly_artists_enabled",
    "pu_books_post_start", "pu_books_post_finish",
    "pu_album_enabled",
    "pu_abs_enabled", "pu_abs_finished_enabled",
    "pu_nd_star_toot_enabled",
]


@app.post("/settings/services")
async def settings_services(request: Request):
    auth = _require_auth(request)
    if auth:
        return auth
    form = await request.form()
    with get_db() as conn:
        for key in SERVICES_SETTINGS_KEYS:
            value = str(form.get(key, "")).strip()
            # Never overwrite a stored secret with an empty submission
            if not value and key in SERVICES_SECRET_KEYS:
                continue
            set_setting(conn, key, value)
    profile_updater.stop()
    profile_updater.start()
    return RedirectResponse(url="/settings?saved=1#services", status_code=302)


@app.post("/settings/auto-toots")
async def settings_auto_toots(request: Request):
    auth = _require_auth(request)
    if auth:
        return auth
    form = await request.form()
    with get_db() as conn:
        for key in AUTO_TOOTS_SETTINGS_KEYS:
            set_setting(conn, key, str(form.get(key, "")).strip())
        for key in AUTO_TOOTS_CHECKBOX_KEYS:
            set_setting(conn, key, "1" if form.get(key) else "0")
    profile_updater.stop()
    profile_updater.start()
    return RedirectResponse(url="/settings?saved=1#toots-updater", status_code=302)


@app.get("/tools")
async def tools_redirect():
    """Redirect old /tools URL to /settings."""
    return RedirectResponse(url="/settings", status_code=301)


@app.post("/settings/profile-updater")
async def settings_profile_updater(request: Request):
    auth = _require_auth(request)
    if auth:
        return auth
    form = await request.form()
    with get_db() as conn:
        for key in PU_SETTINGS_KEYS:
            value = str(form.get(key, "")).strip()
            set_setting(conn, key, value)
        # Checkboxes: absent from form means unchecked
        for key in PU_CHECKBOX_KEYS:
            set_setting(conn, key, "1" if form.get(key) else "0")
        set_setting(conn, "pu_enabled", "1")

    # Restart the updater with new settings
    profile_updater.stop()
    profile_updater.start()

    return RedirectResponse(url="/settings?saved=1#profile-fields", status_code=302)


@app.post("/api/tools/start")
async def api_tools_start(request: Request):
    if (auth := _require_auth_api(request)):
        return auth
    if profile_updater.running:
        return JSONResponse({"status": "ok", "message": "Already running"})
    with get_db() as conn:
        set_setting(conn, "pu_enabled", "1")
    profile_updater.start()
    return JSONResponse({"status": "ok"})


@app.post("/api/tools/stop")
async def api_tools_stop(request: Request):
    if (auth := _require_auth_api(request)):
        return auth
    profile_updater.stop()
    with get_db() as conn:
        set_setting(conn, "pu_enabled", "0")
    return JSONResponse({"status": "ok"})


@app.get("/api/tools/status")
async def api_tools_status(request: Request):
    if (auth := _require_auth_api(request)):
        return auth
    return JSONResponse(profile_updater.get_status())


@app.post("/api/tools/order")
async def api_tools_order(request: Request):
    """Save the field display order."""
    if (auth := _require_auth_api(request)):
        return auth
    data = await request.json()
    order = data.get("order", [])
    if order:
        with get_db() as conn:
            set_setting(conn, "pu_field_order", ",".join(order))
    return JSONResponse({"status": "ok"})
