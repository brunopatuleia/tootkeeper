"""
Profile Updater — updates Mastodon profile fields with now-playing music,
last-watched movie, and last-read book.

Adapted from the standalone mastodon_profile_update project.
"""

import hashlib
import logging
import os
import re
import threading
import time
from typing import Any, Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from mastodon import Mastodon, MastodonError

from app.database import get_all_settings, get_db, get_setting, set_setting

logger = logging.getLogger(__name__)

# ── Media source clients ─────────────────────────────────────────


class LastFmClient:
    API_URL = "https://ws.audioscrobbler.com/2.0/"

    def __init__(self, api_key: str, username: str):
        self.api_key = api_key
        self.username = username

    def get_recent_track(self) -> Optional[dict]:
        params = {
            "method": "user.getrecenttracks",
            "user": self.username,
            "api_key": self.api_key,
            "format": "json",
            "limit": 1,
        }
        try:
            resp = requests.get(self.API_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            tracks = data.get("recenttracks", {}).get("track")
            if not tracks:
                return None
            track = tracks[0] if isinstance(tracks, list) else tracks
            return {
                "artist": track.get("artist", {}).get("#text", "Unknown Artist"),
                "title": track.get("name", "Unknown Title"),
                "now_playing": track.get("@attr", {}).get("nowplaying", "false") == "true",
                "source": "lastfm",
            }
        except Exception as e:
            logger.error(f"Last.fm API failed: {e}")
            return None


class ListenBrainzClient:
    API_URL = "https://api.listenbrainz.org/1"

    def __init__(self, username: str, token: str | None = None):
        self.username = username
        self.token = token

    def get_recent_track(self) -> Optional[dict]:
        endpoint = f"{self.API_URL}/user/{self.username}/listens"
        headers = {}
        if self.token:
            headers["Authorization"] = f"Token {self.token}"
        try:
            resp = requests.get(endpoint, params={"count": 1}, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            listens = data.get("payload", {}).get("listens")
            if not listens:
                return None
            listen = listens[0]
            meta = listen["track_metadata"]
            return {
                "artist": meta.get("artist_name", "Unknown Artist"),
                "title": meta.get("track_name", "Unknown Title"),
                "now_playing": listen.get("playing_now", False),
                "source": "listenbrainz",
            }
        except Exception as e:
            logger.error(f"ListenBrainz API failed: {e}")
            return None


class NavidromeClient:
    """Fetches now-playing / recent track via the Subsonic API (Navidrome-compatible)."""

    def __init__(self, server_url: str, username: str, password: str):
        self.server_url = server_url.rstrip("/")
        self.username = username
        self.password = password

    def _auth_params(self) -> dict:
        salt = os.urandom(8).hex()
        token = hashlib.md5((self.password + salt).encode()).hexdigest()
        return {
            "u": self.username,
            "t": token,
            "s": salt,
            "v": "1.16.1",
            "c": "tootkeeper",
            "f": "json",
        }

    def _api_url(self, endpoint: str) -> str:
        """Build the full API URL, handling servers with or without /rest/ path."""
        base = self.server_url
        # If the user already included /rest or a subpath, use as-is
        if base.endswith("/rest") or "/rest/" in base:
            return f"{base}/{endpoint}"
        return f"{base}/rest/{endpoint}"

    def _parse_response(self, resp: requests.Response) -> Optional[dict]:
        """Parse Subsonic API response, handling both JSON and XML responses."""
        content_type = resp.headers.get("content-type", "")
        text = resp.text.strip()
        if not text:
            logger.error("Navidrome returned empty response")
            return None
        # Try JSON first
        if "json" in content_type or text.startswith("{"):
            data = resp.json().get("subsonic-response", {})
            if data.get("status") != "ok":
                msg = data.get("error", {}).get("message", "Unknown error")
                logger.error(f"Navidrome API error: {msg}")
                return None
            return data
        # Likely HTML login page or error page
        logger.error(f"Navidrome returned non-JSON response (content-type: {content_type}). Check your server URL and credentials.")
        return None

    def get_recent_track(self) -> Optional[dict]:
        try:
            # Try getNowPlaying first
            params = self._auth_params()
            resp = requests.get(self._api_url("getNowPlaying"), params=params, timeout=10)
            resp.raise_for_status()
            data = self._parse_response(resp)
            if data is None:
                return None

            entries = data.get("nowPlaying", {}).get("entry", [])
            if entries:
                entry = entries[0] if isinstance(entries, list) else entries
                return {
                    "artist": entry.get("artist", "Unknown Artist"),
                    "title": entry.get("title", "Unknown Title"),
                    "now_playing": True,
                    "source": "navidrome",
                }

            # Nothing playing now — try getPlayQueue for last played
            params = self._auth_params()
            resp = requests.get(self._api_url("getPlayQueue"), params=params, timeout=10)
            resp.raise_for_status()
            data = self._parse_response(resp)
            if data:
                pq = data.get("playQueue", {})
                entries = pq.get("entry", [])
                if entries:
                    entry = entries[0] if isinstance(entries, list) else entries
                    return {
                        "artist": entry.get("artist", "Unknown Artist"),
                        "title": entry.get("title", "Unknown Title"),
                        "now_playing": False,
                        "source": "navidrome",
                    }

            return None
        except Exception as e:
            logger.error(f"Navidrome API failed: {e}")
            return None


class LetterboxdClient:
    def __init__(self, rss_url: str):
        self.rss_url = rss_url

    def get_recent_movie(self) -> Optional[dict]:
        try:
            feed = feedparser.parse(self.rss_url)
            if not feed.entries:
                return None
            entry = feed.entries[0]
            film_title = film_year = None
            rating = None
            for key, value in entry.items():
                if "filmtitle" in key.lower():
                    film_title = value
                elif "filmyear" in key.lower():
                    film_year = value
                elif "memberrating" in key.lower():
                    rating = float(value)
            if not film_title:
                return None
            return {"title": film_title, "year": film_year or "Unknown", "rating": rating}
        except Exception as e:
            logger.error(f"Letterboxd RSS failed: {e}")
            return None


class GoodreadsClient:
    def __init__(self, rss_url: str):
        self.rss_url = rss_url

    def get_finished_book(self) -> Optional[dict]:
        try:
            feed = feedparser.parse(self.rss_url)
            if not feed.entries:
                return None
            for entry in feed.entries:
                description = entry.get("description", "")
                rating_match = re.search(r"gave (\d+(?:\.\d+)?) stars? to", description, re.IGNORECASE)
                if not rating_match:
                    continue
                rating = float(rating_match.group(1))
                soup = BeautifulSoup(description, "html.parser")
                title_elem = soup.find("a", class_="bookTitle")
                author_elem = soup.find("a", class_="authorName")
                if title_elem and author_elem:
                    return {
                        "title": title_elem.get_text(strip=True),
                        "author": author_elem.get_text(strip=True),
                        "rating": rating,
                    }
            return None
        except Exception as e:
            logger.error(f"Goodreads RSS failed: {e}")
            return None


def _format_stars(rating: float | None) -> str:
    if rating is None:
        return ""
    full = int(rating)
    half = (rating % 1) >= 0.5
    return "★" * full + ("½" if half else "")


# ── Profile Updater ──────────────────────────────────────────────

# Default settings
DEFAULTS = {
    "pu_music_field_name": "NOW PLAYING",
    "pu_movie_field_name": "LAST MOVIE",
    "pu_book_field_name": "LAST BOOK",
    "pu_music_interval": "60",
    "pu_movie_interval": "21600",
    "pu_book_interval": "21600",
    "pu_show_emoji": "1",
    "pu_offline_message": "Nothing playing",
}


def _s(settings: dict, key: str) -> str:
    """Get a profile-updater setting with defaults."""
    return settings.get(key) or DEFAULTS.get(key, "")


class ProfileUpdater:
    def __init__(self):
        self.running = False
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        # Status tracking
        self.last_track_info: str | None = None
        self.last_movie_info: str | None = None
        self.last_book_info: str | None = None
        self.last_custom_info: str | None = None
        self.last_music_update: float = 0
        self.last_movie_update: float = 0
        self.last_book_update: float = 0
        self.error: str | None = None

    def start(self):
        if self.running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.running = True
        logger.info("Profile updater started")

    def stop(self):
        if not self.running:
            return
        self._stop_event.set()
        self.running = False
        logger.info("Profile updater stopped")

    def get_status(self) -> dict:
        return {
            "running": self.running,
            "last_track": self.last_track_info,
            "last_movie": self.last_movie_info,
            "last_book": self.last_book_info,
            "last_custom": self.last_custom_info,
            "last_music_update": self.last_music_update,
            "last_movie_update": self.last_movie_update,
            "last_book_update": self.last_book_update,
            "error": self.error,
        }

    def _build_clients(self, settings: dict) -> tuple:
        """Build music clients, letterboxd, goodreads from settings (only if enabled)."""
        music_clients = []

        if settings.get("pu_music_enabled") == "1":
            # Last.fm
            lfm_user = settings.get("pu_lastfm_username", "").strip()
            lfm_key = settings.get("pu_lastfm_api_key", "").strip()
            if lfm_user and lfm_key:
                music_clients.append(LastFmClient(lfm_key, lfm_user))

            # ListenBrainz
            lb_user = settings.get("pu_listenbrainz_username", "").strip()
            lb_token = settings.get("pu_listenbrainz_token", "").strip()
            if lb_user:
                music_clients.append(ListenBrainzClient(lb_user, lb_token or None))

            # Navidrome (Subsonic API)
            nd_url = settings.get("pu_navidrome_url", "").strip()
            nd_user = settings.get("pu_navidrome_username", "").strip()
            nd_pass = settings.get("pu_navidrome_password", "").strip()
            if nd_url and nd_user and nd_pass:
                music_clients.append(NavidromeClient(nd_url, nd_user, nd_pass))

        # Letterboxd
        letterboxd = None
        if settings.get("pu_movies_enabled") == "1":
            lb_rss = settings.get("pu_letterboxd_rss_url", "").strip()
            if lb_rss:
                letterboxd = LetterboxdClient(lb_rss)

        # Goodreads
        goodreads = None
        if settings.get("pu_books_enabled") == "1":
            gr_rss = settings.get("pu_goodreads_rss_url", "").strip()
            if gr_rss:
                goodreads = GoodreadsClient(gr_rss)

        return music_clients, letterboxd, goodreads

    def _get_mastodon_client(self, settings: dict) -> Mastodon | None:
        instance = settings.get("instance_url")
        token = settings.get("access_token")
        if not instance or not token:
            return None
        return Mastodon(access_token=token, api_base_url=instance)

    def _update_profile_fields(self, client: Mastodon, managed_fields: dict[str, str]) -> bool:
        """Update multiple profile fields in a single API call.

        managed_fields: dict of {field_name: value} for fields this tool manages.
        Preserves non-managed fields and respects the configured field order.
        """
        try:
            account = client.account_verify_credentials()
            current_fields = account.get("fields", [])
            managed_names = set(managed_fields.keys())

            # Keep non-managed fields in their current positions
            other_fields = [
                {"name": f["name"], "value": f["value"]}
                for f in current_fields
                if f["name"] not in managed_names
            ]

            # Build ordered managed fields based on pu_field_order
            with get_db() as conn:
                settings = get_all_settings(conn)
            order_str = settings.get("pu_field_order", "music,movies,books,custom")
            ordered_managed = []
            for key in order_str.split(","):
                key = key.strip()
                field_name = None
                if key == "music":
                    field_name = _s(settings, "pu_music_field_name")
                elif key == "movies":
                    field_name = _s(settings, "pu_movie_field_name")
                elif key == "books":
                    field_name = _s(settings, "pu_book_field_name")
                elif key == "custom":
                    field_name = settings.get("pu_custom_field_name", "").strip()
                if field_name and field_name in managed_fields:
                    ordered_managed.append({"name": field_name, "value": managed_fields[field_name]})

            # Combine: managed fields first (in order), then other fields
            new_fields = ordered_managed + other_fields

            # Mastodon max 4 fields
            new_fields = new_fields[:4]

            fields_tuples = [(f["name"], f["value"]) for f in new_fields]
            client.account_update_credentials(fields=fields_tuples)
            return True
        except MastodonError as e:
            logger.error(f"Failed to update profile fields: {e}")
            return False

    def _format_track(self, track: dict | None, settings: dict) -> str | None:
        if not track:
            return None  # Keep showing the last played track
        emoji = "🎵 " if _s(settings, "pu_show_emoji") == "1" else ""
        return f"{emoji}{track['artist']} - {track['title']}"

    def _format_movie(self, movie: dict | None, settings: dict) -> str:
        emoji = "🎬 " if _s(settings, "pu_show_emoji") == "1" else ""
        if not movie:
            return f"{emoji}No recent movies"
        stars = _format_stars(movie.get("rating"))
        rating_str = f" - {stars}" if stars else ""
        return f"{emoji}{movie['title']} ({movie['year']}){rating_str}"

    def _format_book(self, book: dict | None, settings: dict) -> str:
        emoji = "📚 " if _s(settings, "pu_show_emoji") == "1" else ""
        if not book:
            return f"{emoji}No recent books"
        stars = _format_stars(book.get("rating"))
        rating_str = f" - {stars}" if stars else ""
        return f"{emoji}{book['title']} by {book['author']}{rating_str}"

    def _run_loop(self):
        self.error = None
        try:
            with get_db() as conn:
                settings = get_all_settings(conn)

            music_clients, letterboxd, goodreads = self._build_clients(settings)
            mastodon = self._get_mastodon_client(settings)
            custom_enabled = settings.get("pu_custom_enabled") == "1"

            if not mastodon:
                self.error = "Mastodon not configured"
                self.running = False
                return

            if not music_clients and not letterboxd and not goodreads and not custom_enabled:
                self.error = "No sources enabled"
                self.running = False
                return

            music_interval = int(_s(settings, "pu_music_interval"))
            movie_interval = int(_s(settings, "pu_movie_interval"))
            book_interval = int(_s(settings, "pu_book_interval"))
            loop_interval = min(music_interval, 60)

            # Set custom field on first run
            if custom_enabled:
                name = settings.get("pu_custom_field_name", "").strip()
                value = settings.get("pu_custom_field_value", "").strip()
                if name and value:
                    self.last_custom_info = value

            # First iteration: do an initial update with all fields
            needs_update = True

            while not self._stop_event.is_set():
                try:
                    now = time.time()
                    changed = False

                    # Music update
                    if music_clients and now - self.last_music_update >= music_interval:
                        track = None
                        for client in music_clients:
                            track = client.get_recent_track()
                            if track:
                                break
                        track_info = self._format_track(track, settings)
                        # Only update if we got a track (None means nothing playing — keep last track)
                        if track_info and track_info != self.last_track_info:
                            self.last_track_info = track_info
                            changed = True
                            logger.info(f"Music changed: {track_info}")
                        self.last_music_update = now

                    # Movie update
                    if letterboxd and now - self.last_movie_update >= movie_interval:
                        movie = letterboxd.get_recent_movie()
                        movie_info = self._format_movie(movie, settings)
                        if movie_info != self.last_movie_info:
                            self.last_movie_info = movie_info
                            changed = True
                            logger.info(f"Movie changed: {movie_info}")
                        self.last_movie_update = now

                    # Book update
                    if goodreads and now - self.last_book_update >= book_interval:
                        book = goodreads.get_finished_book()
                        book_info = self._format_book(book, settings)
                        if book_info != self.last_book_info:
                            self.last_book_info = book_info
                            changed = True
                            logger.info(f"Book changed: {book_info}")
                        self.last_book_update = now

                    # Push all managed fields in one API call when anything changes
                    if changed or needs_update:
                        managed = {}
                        if self.last_track_info:
                            managed[_s(settings, "pu_music_field_name")] = self.last_track_info
                        if self.last_movie_info:
                            managed[_s(settings, "pu_movie_field_name")] = self.last_movie_info
                        if self.last_book_info:
                            managed[_s(settings, "pu_book_field_name")] = self.last_book_info
                        if self.last_custom_info:
                            name = settings.get("pu_custom_field_name", "").strip()
                            if name:
                                managed[name] = self.last_custom_info
                        if managed:
                            self._update_profile_fields(mastodon, managed)
                        needs_update = False

                except Exception as e:
                    logger.error(f"Profile updater loop error: {e}", exc_info=True)
                    self.error = str(e)

                self._stop_event.wait(loop_interval)

        except Exception as e:
            logger.exception("Profile updater failed to start")
            self.error = str(e)
        finally:
            self.running = False
