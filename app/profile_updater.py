"""
Profile Updater — updates Mastodon profile fields with now-playing music,
last-watched movie, and last-read book.

Adapted from the standalone mastodon_profile_update project.
"""

import base64
import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any, Optional

import feedparser
import requests
from mastodon import Mastodon, MastodonError

from app.config import APP_URL
from app.database import get_all_settings, get_db, get_setting, set_setting

logger = logging.getLogger(__name__)

import socket

_BLOCKED_HOSTS = {"169.254.169.254", "169.254.170.2", "metadata.google.internal"}


def _safe_url(url: str) -> bool:
    """Block cloud metadata endpoints and loopback addresses. Private IPs allowed (homelab)."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        hostname = (parsed.hostname or "").lower()
        if hostname in _BLOCKED_HOSTS:
            return False
        # Block loopback / unspecified addresses by resolving the hostname
        try:
            ip = socket.gethostbyname(hostname)
            if ip.startswith("127.") or ip in ("0.0.0.0", "::1"):
                return False
        except socket.gaierror:
            return False
        return True
    except Exception:
        return False

# ── Media source clients ─────────────────────────────────────────


class _LastFmCompatClient:
    """Shared implementation for Last.fm-compatible scrobbling APIs."""
    API_URL: str = ""
    SOURCE: str = ""

    def __init__(self, username: str, api_key: str = ""):
        self.username = username
        self.api_key = api_key

    def _base_params(self) -> dict:
        p = {"user": self.username, "format": "json"}
        if self.api_key:
            p["api_key"] = self.api_key
        return p

    def get_recent_track(self) -> Optional[dict]:
        params = {**self._base_params(), "method": "user.getrecenttracks", "limit": 1}
        try:
            resp = requests.get(self.API_URL, params=params, timeout=10)
            resp.raise_for_status()
            tracks = resp.json().get("recenttracks", {}).get("track")
            if not tracks:
                return None
            track = tracks[0] if isinstance(tracks, list) else tracks
            return {
                "artist": track.get("artist", {}).get("#text", "Unknown Artist"),
                "title": track.get("name", "Unknown Title"),
                "now_playing": track.get("@attr", {}).get("nowplaying", "false") == "true",
                "source": self.SOURCE,
            }
        except Exception as e:
            logger.error(f"{self.SOURCE} API failed: {e}")
            return None

    def get_top_artists_weekly(self, limit: int = 5) -> list[dict]:
        params = {**self._base_params(), "method": "user.getTopArtists", "period": "7day", "limit": limit}
        try:
            resp = requests.get(self.API_URL, params=params, timeout=10)
            resp.raise_for_status()
            artists = resp.json().get("topartists", {}).get("artist", [])
            if not isinstance(artists, list):
                artists = [artists]
            return [
                {"name": a.get("name", "Unknown"), "playcount": int(a.get("playcount", 0))}
                for a in artists[:limit]
            ]
        except Exception as e:
            logger.error(f"{self.SOURCE} top artists failed: {e}")
            return []


class LastFmClient(_LastFmCompatClient):
    API_URL = "https://ws.audioscrobbler.com/2.0/"
    SOURCE = "lastfm"

    def __init__(self, api_key: str, username: str):
        super().__init__(username=username, api_key=api_key)


class LibreFmClient(_LastFmCompatClient):
    """libre.fm — open-source Last.fm-compatible service. No API key needed."""
    API_URL = "https://libre.fm/2.0/"
    SOURCE = "librefm"

    def __init__(self, username: str):
        super().__init__(username=username)


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

    def get_top_artists_weekly(self, limit: int = 5) -> list[dict]:
        endpoint = f"{self.API_URL}/user/{self.username}/stats/top-artists"
        headers = {}
        if self.token:
            headers["Authorization"] = f"Token {self.token}"
        try:
            resp = requests.get(endpoint, params={"range": "week", "count": limit}, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            artists = data.get("payload", {}).get("artists", [])
            return [
                {"name": a.get("artist_name", "Unknown"), "playcount": a.get("listen_count", 0)}
                for a in artists[:limit]
            ]
        except Exception as e:
            logger.error(f"ListenBrainz top artists failed: {e}")
            return []


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
            "c": "mastoferr",
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

    def get_album_info(self, album_id: str) -> dict | None:
        """Fetch album metadata and track count via getAlbum."""
        params = self._auth_params()
        params["id"] = album_id
        try:
            resp = requests.get(self._api_url("getAlbum"), params=params, timeout=10)
            resp.raise_for_status()
            data = self._parse_response(resp)
            if not data:
                return None
            album = data.get("album", {})
            songs = album.get("song", [])
            if not isinstance(songs, list):
                songs = [songs] if songs else []
            # Count unique (disc, track) pairs — handles multi-disc albums
            track_keys = {(s.get("discNumber", 1), s.get("track", 0)) for s in songs}
            total_tracks = len(track_keys) or album.get("songCount", 0)
            # Genres: OpenSubsonic returns [{name:...}], standard returns a string
            genres: list[str] = []
            raw_genres = album.get("genres")
            if raw_genres:
                for g in (raw_genres if isinstance(raw_genres, list) else [raw_genres]):
                    genres.append(g["name"] if isinstance(g, dict) else g)
            elif album.get("genre"):
                genres = [album["genre"]]
            return {
                "id": album_id,
                "name": album.get("name", "Unknown Album"),
                "artist": album.get("artist", "Unknown Artist"),
                "year": str(album["year"]) if album.get("year") else "",
                "genres": [g for g in genres if g],
                "cover_art_id": album.get("coverArt") or album_id,
                "total_tracks": total_tracks,
            }
        except Exception as e:
            logger.error(f"Navidrome getAlbum failed ({album_id}): {e}")
            return None

    def get_cover_art_bytes(self, cover_art_id: str) -> bytes | None:
        """Download album cover art."""
        params = self._auth_params()
        params["id"] = cover_art_id
        try:
            resp = requests.get(self._api_url("getCoverArt"), params=params, timeout=15)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.error(f"Navidrome getCoverArt failed ({cover_art_id}): {e}")
            return None

    def get_top_artists_weekly(self, limit: int = 5) -> list[dict]:
        """Get top artists from the last 7 days by aggregating recent album plays."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        params = self._auth_params()
        params.update({"type": "recent", "size": "500"})
        try:
            resp = requests.get(self._api_url("getAlbumList2"), params=params, timeout=15)
            resp.raise_for_status()
            data = self._parse_response(resp)
            if not data:
                return []
            albums = data.get("albumList2", {}).get("album", [])
            if not isinstance(albums, list):
                albums = [albums]

            artist_plays: dict[str, int] = defaultdict(int)
            for album in albums:
                played_str = album.get("played")
                artist = album.get("artist", "Unknown")
                if played_str:
                    try:
                        played = datetime.fromisoformat(played_str.replace("Z", "+00:00"))
                        if played < cutoff:
                            continue
                    except (ValueError, TypeError):
                        pass
                artist_plays[artist] += album.get("playCount", 1)

            sorted_artists = sorted(artist_plays.items(), key=lambda x: x[1], reverse=True)
            return [{"name": name, "playcount": count} for name, count in sorted_artists[:limit]]
        except Exception as e:
            logger.error(f"Navidrome top artists failed: {e}")
            return []

    def get_starred_songs(self) -> list[dict]:
        """Return all currently starred/loved songs from Navidrome."""
        try:
            params = self._auth_params()
            resp = requests.get(self._api_url("getStarred2"), params=params, timeout=10)
            data = self._parse_response(resp)
            if not data:
                return []
            songs = data.get("starred2", {}).get("song", [])
            if isinstance(songs, dict):
                songs = [songs]
            return songs or []
        except Exception as e:
            logger.error(f"Navidrome getStarred2 failed: {e}")
            return []

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
                    "albumId": entry.get("albumId"),
                    "album": entry.get("album"),
                    "track": entry.get("track"),
                    "discNumber": entry.get("discNumber", 1),
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
                        "albumId": entry.get("albumId"),
                        "album": entry.get("album"),
                        "track": entry.get("track"),
                        "discNumber": entry.get("discNumber", 1),
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

    @staticmethod
    def _parse_title_author(text: str) -> tuple[str, str] | None:
        """Extract (title, author) from 'Title by Author' using the last ' by '."""
        m = re.search(r"^(.+) by (.+)$", text.strip())
        if m:
            return m.group(1).strip(), m.group(2).strip()
        return None

    def get_finished_book(self) -> Optional[dict]:
        try:
            feed = feedparser.parse(self.rss_url)
            if not feed.entries:
                return None
            for entry in feed.entries:
                title = entry.get("title", "")
                # "Username gave X.XX stars to Book Title by Author"
                m = re.search(r"gave (\d+(?:\.\d+)?) stars? to (.+)", title, re.IGNORECASE)
                if m:
                    rating = float(m.group(1))
                    parsed = self._parse_title_author(m.group(2))
                    if parsed:
                        return {"title": parsed[0], "author": parsed[1], "rating": rating}
            return None
        except Exception as e:
            logger.error(f"Goodreads RSS failed: {e}")
            return None

    def get_book_events(self, since_entry_id: str | None = None) -> list[dict]:
        """Return new book events (started/finished) newer than since_entry_id.

        Events are returned newest-first (RSS order). Callers should reverse
        before posting so events go out in chronological order.
        """
        try:
            feed = feedparser.parse(self.rss_url)
            if not feed.entries:
                return []
            events = []
            for entry in feed.entries:
                entry_id = entry.get("id") or entry.get("link", "")
                if since_entry_id and entry_id == since_entry_id:
                    break
                title = entry.get("title", "")

                # Finished / rated: "Username gave X stars to Title by Author"
                m = re.search(r"gave (\d+(?:\.\d+)?) stars? to (.+)", title, re.IGNORECASE)
                if m:
                    rating = float(m.group(1))
                    parsed = self._parse_title_author(m.group(2))
                    events.append({
                        "type": "finished",
                        "book_title": parsed[0] if parsed else m.group(2).strip(),
                        "author": parsed[1] if parsed else "",
                        "rating": rating,
                        "entry_id": entry_id,
                    })
                    continue

                # Started: "Username is currently reading Title by Author"
                #       or "Username started reading Title by Author"
                m = re.search(
                    r"(?:is currently reading|started reading)\s+(.+)",
                    title, re.IGNORECASE
                )
                if m:
                    parsed = self._parse_title_author(m.group(1))
                    events.append({
                        "type": "started",
                        "book_title": parsed[0] if parsed else m.group(1).strip(),
                        "author": parsed[1] if parsed else "",
                        "rating": None,
                        "entry_id": entry_id,
                    })

            return events
        except Exception as e:
            logger.error(f"Goodreads book events failed: {e}")
            return []


class AudiobookshelfClient:
    def __init__(self, server_url: str, token: str):
        self.server_url = server_url.rstrip("/")
        self.token = token
        self._headers = {"Authorization": f"Bearer {token}"}

    def get_in_progress_books(self) -> list[dict]:
        """Return all books currently in progress."""
        try:
            resp = requests.get(
                f"{self.server_url}/api/me/items-in-progress",
                headers=self._headers,
                timeout=10,
            )
            resp.raise_for_status()
            items = resp.json().get("libraryItems", [])
            return [
                {"libraryItemId": item["id"]}
                for item in items
                if item.get("mediaType") == "book"
            ]
        except Exception as e:
            logger.error(f"Audiobookshelf items-in-progress failed: {e}")
            return []

    def get_book_metadata(self, library_item_id: str) -> dict | None:
        """Fetch title, author, year, and genres for a library item."""
        try:
            resp = requests.get(
                f"{self.server_url}/api/items/{library_item_id}",
                headers=self._headers,
                timeout=10,
            )
            resp.raise_for_status()
            meta = resp.json().get("media", {}).get("metadata", {})
            year = meta.get("publishedYear") or (
                (meta.get("publishedDate") or "")[:4] or None
            )
            return {
                "id": library_item_id,
                "title": meta.get("title") or "Unknown Title",
                "subtitle": meta.get("subtitle") or "",
                "author": meta.get("authorName") or "",
                "narrator": meta.get("narratorName") or "",
                "year": year,
                "genres": meta.get("genres") or [],
            }
        except Exception as e:
            logger.error(f"Audiobookshelf item metadata failed ({library_item_id}): {e}")
            return None

    def get_cover_bytes(self, library_item_id: str) -> bytes | None:
        """Download the book cover image."""
        try:
            resp = requests.get(
                f"{self.server_url}/api/items/{library_item_id}/cover",
                headers=self._headers,
                timeout=15,
            )
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.error(f"Audiobookshelf cover download failed ({library_item_id}): {e}")
            return None

    def create_share_link(self, library_item_id: str, expiry_hours: int = 0) -> str | None:
        """Create a share link for a library item. Returns the slug, or None on failure.

        expiry_hours=0 means permanent.
        """
        try:
            body: dict = {"libraryItemId": library_item_id}
            if expiry_hours and expiry_hours > 0:
                expires_at = int((time.time() + expiry_hours * 3600) * 1000)
                body["expiresAt"] = expires_at
            resp = requests.post(
                f"{self.server_url}/api/share/mediaProgress",
                headers=self._headers,
                json=body,
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json().get("slug")
        except Exception as e:
            logger.error(f"Audiobookshelf share link creation failed ({library_item_id}): {e}")
            return None

    def get_user_progress(self, library_item_id: str) -> dict | None:
        """Fetch user progress for a library item (includes isFinished)."""
        try:
            resp = requests.get(
                f"{self.server_url}/api/me/progress/{library_item_id}",
                headers=self._headers,
                timeout=10,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Audiobookshelf progress check failed ({library_item_id}): {e}")
            return None


class SpotifyClient:
    TOKEN_URL = "https://accounts.spotify.com/api/token"
    API_URL = "https://api.spotify.com/v1"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self._access_token: str | None = None
        self._token_expiry: float = 0

    def _get_access_token(self) -> str | None:
        if self._access_token and time.time() < self._token_expiry - 60:
            return self._access_token
        credentials = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        try:
            resp = requests.post(
                self.TOKEN_URL,
                headers={"Authorization": f"Basic {credentials}"},
                data={"grant_type": "refresh_token", "refresh_token": self.refresh_token},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data["access_token"]
            self._token_expiry = time.time() + data.get("expires_in", 3600)
            return self._access_token
        except Exception as e:
            logger.error(f"Spotify token refresh failed: {e}")
            return None

    def get_recent_track(self) -> Optional[dict]:
        token = self._get_access_token()
        if not token:
            return None
        headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = requests.get(f"{self.API_URL}/me/player/currently-playing", headers=headers, timeout=10)
            if resp.status_code == 200 and resp.content:
                data = resp.json()
                item = data.get("item")
                if item:
                    return {
                        "artist": ", ".join(a["name"] for a in item.get("artists", [])),
                        "title": item["name"],
                        "now_playing": data.get("is_playing", False),
                        "source": "spotify",
                    }
            # Fall back to recently played
            resp = requests.get(f"{self.API_URL}/me/player/recently-played?limit=1", headers=headers, timeout=10)
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                track = items[0]["track"]
                return {
                    "artist": ", ".join(a["name"] for a in track.get("artists", [])),
                    "title": track["name"],
                    "now_playing": False,
                    "source": "spotify",
                }
        except Exception as e:
            logger.error(f"Spotify API failed: {e}")
        return None


class JellyfinClient:
    def __init__(self, server_url: str, api_key: str, user_id: str = ""):
        self.server_url = server_url.rstrip("/")
        self.api_key = api_key
        self.user_id = user_id
        self._headers = {"X-Emby-Token": api_key}

    def get_recent_track(self) -> Optional[dict]:
        try:
            resp = requests.get(f"{self.server_url}/Sessions", headers=self._headers, timeout=10)
            resp.raise_for_status()
            for session in resp.json():
                item = session.get("NowPlayingItem")
                if not item or item.get("Type") != "Audio":
                    continue
                if self.user_id and session.get("UserId") != self.user_id:
                    continue
                artists = item.get("Artists") or item.get("AlbumArtists") or []
                return {
                    "artist": artists[0] if artists else "Unknown Artist",
                    "title": item.get("Name", "Unknown Title"),
                    "now_playing": True,
                    "source": "jellyfin",
                }
        except Exception as e:
            logger.error(f"Jellyfin API failed: {e}")
        return None


class PlexClient:
    def __init__(self, server_url: str, token: str):
        self.server_url = server_url.rstrip("/")
        self.token = token
        self._headers = {"X-Plex-Token": token, "Accept": "application/json"}

    def get_recent_track(self) -> Optional[dict]:
        try:
            resp = requests.get(f"{self.server_url}/status/sessions", headers=self._headers, timeout=10)
            resp.raise_for_status()
            items = resp.json().get("MediaContainer", {}).get("Metadata", [])
            for item in items:
                if item.get("type") == "track":
                    return {
                        "artist": item.get("grandparentTitle", "Unknown Artist"),
                        "title": item.get("title", "Unknown Title"),
                        "now_playing": True,
                        "source": "plex",
                    }
        except Exception as e:
            logger.error(f"Plex API failed: {e}")
        return None


class TautulliClient:
    def __init__(self, server_url: str, api_key: str):
        self.server_url = server_url.rstrip("/")
        self.api_key = api_key

    def get_recent_track(self) -> Optional[dict]:
        try:
            resp = requests.get(
                f"{self.server_url}/api/v2",
                params={"apikey": self.api_key, "cmd": "get_activity"},
                timeout=10,
            )
            resp.raise_for_status()
            sessions = resp.json().get("response", {}).get("data", {}).get("sessions", [])
            for session in sessions:
                if session.get("media_type") == "track":
                    return {
                        "artist": session.get("grandparent_title", "Unknown Artist"),
                        "title": session.get("title", "Unknown Title"),
                        "now_playing": True,
                        "source": "tautulli",
                    }
        except Exception as e:
            logger.error(f"Tautulli API failed: {e}")
        return None


def _format_stars(rating: float | None) -> str:
    if rating is None:
        return ""
    full = int(rating)
    half = (rating % 1) >= 0.5
    return "★" * full + ("½" if half else "")


def _get_top_artists_weekly(music_clients: list, limit: int = 5) -> list[dict]:
    """Try each configured music client until one returns weekly top artists."""
    for client in music_clients:
        if hasattr(client, "get_top_artists_weekly"):
            artists = client.get_top_artists_weekly(limit=limit)
            if artists:
                return artists
    return []


def _format_weekly_artists_toot(artists: list[dict], settings: dict) -> str:
    show_emoji = _s(settings, "pu_show_emoji") == "1"
    header = "🎵 My top 5 artists this week:" if show_emoji else "My top 5 artists this week:"
    hashtags = settings.get("pu_weekly_artists_hashtags", "").strip() or "#music #weeklyrecap"
    lines = [header, ""]
    for i, artist in enumerate(artists, 1):
        count = artist.get("playcount", 0)
        count_str = f" ({count} plays)" if count else ""
        lines.append(f"{i}. {artist['name']}{count_str}")
    lines.append("")
    lines.append(hashtags)
    return "\n".join(lines)


def _format_book_started_toot(event: dict, settings: dict) -> str:
    emoji = "📚 " if _s(settings, "pu_show_emoji") == "1" else ""
    author = event.get("author", "")
    author_str = f" by {author}" if author else ""
    hashtags = settings.get("pu_books_hashtags", "").strip() or "#books #amreading"

    template = settings.get("pu_books_start_template", "").strip()
    if template:
        substitutions = {
            "Title": event.get("book_title", ""),
            "Author": author,
            "AuthorLine": f"by {author}" if author else "",
            "Hashtags": hashtags,
        }
        return _render_template(template, substitutions)

    return f"{emoji}Just started reading: {event['book_title']}{author_str}\n\n{hashtags}"


def _format_book_finished_toot(event: dict, settings: dict) -> str:
    emoji = "📚 " if _s(settings, "pu_show_emoji") == "1" else ""
    author = event.get("author", "")
    author_str = f" by {author}" if author else ""
    stars = _format_stars(event.get("rating"))
    rating_str = f" — {stars}" if stars else ""
    hashtags = settings.get("pu_books_hashtags", "").strip() or "#books #bookworm"

    template = settings.get("pu_books_finish_template", "").strip()
    if template:
        substitutions = {
            "Title": event.get("book_title", ""),
            "Author": author,
            "AuthorLine": f"by {author}" if author else "",
            "Rating": stars,
            "RatingSuffix": f" — {stars}" if stars else "",
            "Hashtags": hashtags,
        }
        return _render_template(template, substitutions)

    return f"{emoji}Just finished reading: {event['book_title']}{author_str}{rating_str}\n\n{hashtags}"


def _format_album_toot(album: dict, settings: dict) -> str:
    """Format a toot for a completed album listen session."""
    artist = album.get("artist", "Unknown Artist")
    name = album.get("name", "Unknown Album")
    year = str(album.get("year", "")) if album.get("year") else ""
    genres = album.get("genres", [])

    album_line = f"[{year}] {name}" if year else name
    genre_tags = " ".join(_genre_to_hashtag(g) for g in genres[:5])
    base_tags = settings.get("pu_album_hashtags", "").strip() or "#NowPlaying"
    hashtags = f"{base_tags} {genre_tags}".strip() if genre_tags else base_tags

    template = settings.get("pu_album_template", "").strip()
    if template:
        substitutions = {
            "Artist": artist,
            "Album": name,
            "Year": year,
            "AlbumLine": album_line,
            "Hashtags": hashtags,
            **_build_genre_vars(genres),
        }
        return _render_template(template, substitutions)

    return "\n".join([artist, album_line, "", hashtags])


def _format_starred_toot(song: dict, settings: dict) -> str:
    """Format a toot for a newly starred Navidrome track."""
    artist = song.get("artist", "Unknown Artist")
    title = song.get("title", "Unknown Title")
    album = song.get("album", "")
    year = str(song.get("year", "")) if song.get("year") else ""
    genre = song.get("genre", "")
    genre_tag = _genre_to_hashtag(genre) if genre else ""
    hashtags = f"#NowPlaying {genre_tag}".strip() if genre_tag else "#NowPlaying"

    template = settings.get("pu_star_template", "").strip()
    if template:
        mbid = song.get("musicBrainzId", "")
        song_link = _get_odesli_url(mbid) if (mbid and "%SongLink%" in template) else ""
        lfm_key = settings.get("pu_lastfm_api_key", "").strip()
        similar_artists = _get_similar_artists(artist, lfm_key) if (lfm_key and "%SimilarArtists%" in template) else ""
        substitutions = {
            "Artist": artist,
            "Title": title,
            "Album": album,
            "Year": year,
            "GenreTag": genre_tag,
            "GenreTags": genre_tag,
            "SongLink": song_link,
            "SimilarArtists": similar_artists,
            "Hashtags": hashtags,
        }
        return _render_template(template, substitutions)

    return f"{artist} - {title}\n\n{hashtags}"


# ── Pending toot confirmation store ──────────────────────────────────────────
# Keyed by a UUID token. Each entry holds everything needed to post later.
_pending_toots: dict[str, dict] = {}
_pending_lock = threading.Lock()


def _queue_pending_toot(
    label: str,
    text: str,
    cover_bytes: bytes | None,
    cover_mime: str,
    cover_desc: str,
    ttl_seconds: int = 86400,
) -> str:
    """Store a toot for later confirmation. Returns the opaque token."""
    token = str(uuid.uuid4())
    with _pending_lock:
        _pending_toots[token] = {
            "label": label,
            "text": text,
            "cover_bytes": cover_bytes,
            "cover_mime": cover_mime,
            "cover_desc": cover_desc,
            "expires": time.time() + ttl_seconds,
        }
    return token


def pop_pending_toot(token: str) -> dict | None:
    """Remove and return a pending toot entry, or None if missing/expired."""
    with _pending_lock:
        entry = _pending_toots.pop(token, None)
    if entry and time.time() > entry["expires"]:
        return None
    return entry


def _expire_pending_toots() -> None:
    """Remove all expired entries. Call periodically from the updater loop."""
    now = time.time()
    with _pending_lock:
        expired = [t for t, e in _pending_toots.items() if now > e["expires"]]
        for t in expired:
            del _pending_toots[t]


def _safe_webhook_url(url: str) -> bool:
    """Validate a webhook URL — must be http/https and not resolve to loopback or metadata endpoints."""
    from urllib.parse import urlparse
    _BLOCKED = {"169.254.169.254", "169.254.170.2", "metadata.google.internal"}
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        hostname = (parsed.hostname or "").lower()
        if hostname in _BLOCKED:
            return False
        ip = socket.gethostbyname(hostname)
        if ip.startswith("127.") or ip in ("0.0.0.0", "::1"):
            return False
        return True
    except Exception:
        return False


def _send_discord_confirmation(
    webhook_url: str,
    label: str,
    toot_text: str,
    confirm_url: str,
) -> None:
    """Send a Discord message asking for toot confirmation."""
    if not _safe_webhook_url(webhook_url):
        logger.error("Discord webhook URL failed safety check — skipping")
        return
    content = (
        f"**New toot ready to post** — {label}\n\n"
        f"```\n{toot_text}\n```\n"
        f"[Confirm and post]({confirm_url})"
    )
    try:
        resp = requests.post(
            webhook_url,
            json={"content": content},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Discord confirmation webhook failed: {e}")


def _genre_to_hashtag(genre: str) -> str:
    """Convert a genre string to a hashtag, stripping non-alphanumeric characters."""
    cleaned = re.sub(r"[^a-zA-Z0-9\s]", " ", genre).strip()
    return "#" + "".join(w.capitalize() for w in cleaned.split())


def _build_genre_vars(genres: list, base_count: int = 5) -> dict:
    """Build %GenreTags% and %GenreTags:N% variable dict entries."""
    tags = [_genre_to_hashtag(g) for g in genres[:base_count]]
    result = {"GenreTags": " ".join(tags)}
    for n in range(1, base_count + 1):
        result[f"GenreTags:{n}"] = " ".join(tags[:n])
    return result


def _render_template(template: str, substitutions: dict) -> str:
    """Render a toot template substituting %Variable% placeholders.

    Lines that were non-blank in the template but resolve to blank after
    substitution are dropped (handles optional vars like %Subtitle%).
    Multiple consecutive blank lines are collapsed to one.
    """
    result_lines = []
    # Sort longest keys first so %GenreTags:3% is replaced before %GenreTags%
    sorted_keys = sorted(substitutions.keys(), key=len, reverse=True)
    for line in template.split("\n"):
        substituted = line
        for key in sorted_keys:
            substituted = substituted.replace(f"%{key}%", substitutions[key] or "")
        stripped = substituted.strip()
        # Drop lines that had content (variables) but resolved to nothing
        if stripped == "" and line.strip() != "":
            continue
        result_lines.append(stripped)
    text = "\n".join(result_lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _get_odesli_url(mbid: str) -> str:
    """Look up a Songlink/Odesli URL for a MusicBrainz recording ID."""
    if not mbid:
        return ""
    try:
        resp = requests.get(
            "https://api.song.link/v1-alpha.1/links",
            params={"url": f"https://musicbrainz.org/recording/{mbid}", "userCountry": "US"},
            timeout=8,
        )
        if resp.status_code == 200:
            return resp.json().get("pageUrl", "")
    except Exception as e:
        logger.debug(f"Odesli lookup failed for {mbid}: {e}")
    return ""


def _get_similar_artists(artist: str, api_key: str, limit: int = 3) -> str:
    """Return a comma-separated list of similar artists from Last.fm (top N)."""
    if not artist or not api_key:
        return ""
    try:
        resp = requests.get(
            "https://ws.audioscrobbler.com/2.0/",
            params={
                "method": "artist.getSimilar",
                "artist": artist,
                "api_key": api_key,
                "format": "json",
                "limit": limit,
            },
            timeout=8,
        )
        if resp.status_code == 200:
            data = resp.json()
            artists = data.get("similarartists", {}).get("artist", [])
            names = [a["name"] for a in artists[:limit] if "name" in a]
            return ", ".join(names)
    except Exception as e:
        logger.debug(f"Last.fm getSimilar failed for {artist!r}: {e}")
    return ""


def _abs_vars(book: dict, settings: dict, share_url: str, hashtags: str, genres: list) -> dict:
    """Build template variable dict for ABS toots."""
    author = book.get("author", "")
    narrator = book.get("narrator", "")
    year = str(book.get("year", "")) if book.get("year") else ""
    return {
        "Title": book.get("title", "Unknown"),
        "Subtitle": book.get("subtitle", ""),
        "Author": author,
        "AuthorLine": f"by {author}" if author else "",
        "Narrator": narrator,
        "NarratorLine": f"narrated by {narrator}" if narrator else "",
        "Year": year,
        "YearBracketed": f"[{year}]" if year else "",
        "ShareLink": share_url,
        "Hashtags": hashtags,
        **_build_genre_vars(genres),
    }


def _format_abs_toot(book: dict, settings: dict, share_url: str = "") -> str:
    """Format a toot for a newly started Audiobookshelf book."""
    title = book.get("title", "Unknown")
    subtitle = book.get("subtitle", "")
    author = book.get("author", "")
    narrator = book.get("narrator", "")
    year = book.get("year", "")
    genres = book.get("genres", [])

    genre_tags = " ".join(_genre_to_hashtag(g) for g in genres[:5])
    base_tags = settings.get("pu_abs_hashtags", "").strip() or "#NowReading #Audiobooks #Books"
    hashtags = f"{base_tags} {genre_tags}".strip() if genre_tags else base_tags

    template = settings.get("pu_abs_template", "").strip()
    if template:
        return _render_template(template, _abs_vars(book, settings, share_url, hashtags, genres))

    parts = [title]
    if subtitle:
        parts.append(subtitle)
    parts.append("")
    by_line = f"by {author}" if author else ""
    narrated_line = f"narrated by {narrator}" if narrator else ""
    meta_parts = [p for p in [by_line, narrated_line] if p]
    if year:
        meta_parts.append(f"[{year}]")
    if meta_parts:
        parts.append(" ".join(meta_parts))
    if share_url:
        parts.append("")
        parts.append(share_url)
    parts.append("")
    parts.append(hashtags)
    return "\n".join(parts)


def _format_abs_finished_toot(book: dict, settings: dict, share_url: str = "") -> str:
    """Format a toot for a finished Audiobookshelf audiobook."""
    title = book.get("title", "Unknown")
    subtitle = book.get("subtitle", "")
    author = book.get("author", "")
    narrator = book.get("narrator", "")
    year = book.get("year", "")
    genres = book.get("genres", [])

    genre_tags = " ".join(_genre_to_hashtag(g) for g in genres[:5])
    base_tags = settings.get("pu_abs_finished_hashtags", "").strip() or "#FinishedReading #Audiobooks #Books"
    hashtags = f"{base_tags} {genre_tags}".strip() if genre_tags else base_tags

    template = settings.get("pu_abs_finished_template", "").strip()
    if template:
        substitutions = _abs_vars(book, settings, share_url, hashtags, genres)
        substitutions["Title"] = f"Just finished: {title}"
        return _render_template(template, substitutions)

    parts = [f"Just finished: {title}"]
    if subtitle:
        parts.append(subtitle)
    parts.append("")
    by_line = f"by {author}" if author else ""
    narrated_line = f"narrated by {narrator}" if narrator else ""
    meta_parts = [p for p in [by_line, narrated_line] if p]
    if year:
        meta_parts.append(f"[{year}]")
    if meta_parts:
        parts.append(" ".join(meta_parts))
    if share_url:
        parts.append("")
        parts.append(share_url)
    parts.append("")
    parts.append(hashtags)
    return "\n".join(parts)


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
    "pu_abs_interval": "900",
    "pu_abs_hashtags": "#NowReading #Audiobooks #Books",
    "pu_abs_finished_hashtags": "#FinishedReading #Audiobooks #Books",
    "pu_album_hashtags": "#NowPlaying",
    "pu_weekly_artists_day": "0",
    "pu_weekly_artists_hour": "0",
}


def _s(settings: dict, key: str) -> str:
    """Get a profile-updater setting with defaults."""
    return settings.get(key) or DEFAULTS.get(key, "")


class ProfileUpdater:
    def __init__(self):
        self.running = False
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._post_lock = threading.Lock()  # prevents duplicate posts if two threads overlap
        # Status tracking — restore persisted values (DB may not exist yet on first boot)
        try:
            with get_db() as conn:
                self.last_track_info: str | None = get_setting(conn, "pu_last_track_info")
                self.last_movie_info: str | None = get_setting(conn, "pu_last_movie_info")
                self.last_book_info: str | None = get_setting(conn, "pu_last_book_info")
        except Exception:
            self.last_track_info = None
            self.last_movie_info = None
            self.last_book_info = None
        self.last_custom_info: str | None = None
        self._cached_fields: list | None = None  # cached from account_verify_credentials
        self.last_music_update: float = 0
        self.last_movie_update: float = 0
        self.last_book_update: float = 0
        self.last_abs_update: float = 0
        self._album_session: dict | None = self._load_album_session()
        self.error: str | None = None

    def start(self):
        # If a previous thread is still alive (e.g. stop() was just called),
        # wait briefly for it to exit before spawning a new one.  Without this,
        # stop()+start() on settings-save can leave two threads running at the
        # same time, both able to fire the same toot.
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)
        if self._thread is not None and self._thread.is_alive():
            logger.warning("Profile updater: old thread still alive after 5 s, proceeding anyway")
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
        """Build music clients, letterboxd, goodreads, and audiobookshelf from settings."""
        music_clients = []

        if settings.get("pu_music_enabled") == "1":
            # Last.fm
            lfm_user = settings.get("pu_lastfm_username", "").strip()
            lfm_key = settings.get("pu_lastfm_api_key", "").strip()
            if lfm_user and lfm_key:
                music_clients.append(LastFmClient(lfm_key, lfm_user))

            # libre.fm
            lfree_user = settings.get("pu_librefm_username", "").strip()
            if lfree_user:
                music_clients.append(LibreFmClient(lfree_user))

            # ListenBrainz
            lb_user = settings.get("pu_listenbrainz_username", "").strip()
            lb_token = settings.get("pu_listenbrainz_token", "").strip()
            if lb_user:
                music_clients.append(ListenBrainzClient(lb_user, lb_token or None))

            # Navidrome (Subsonic API)
            nd_url = settings.get("pu_navidrome_url", "").strip()
            nd_user = settings.get("pu_navidrome_username", "").strip()
            nd_pass = settings.get("pu_navidrome_password", "").strip()
            if nd_url and nd_user and nd_pass and _safe_url(nd_url):
                music_clients.append(NavidromeClient(nd_url, nd_user, nd_pass))

            # Spotify
            sp_id = settings.get("pu_spotify_client_id", "").strip()
            sp_secret = settings.get("pu_spotify_client_secret", "").strip()
            sp_refresh = settings.get("pu_spotify_refresh_token", "").strip()
            if sp_id and sp_secret and sp_refresh:
                music_clients.append(SpotifyClient(sp_id, sp_secret, sp_refresh))

            # Jellyfin
            jf_url = settings.get("pu_jellyfin_url", "").strip()
            jf_key = settings.get("pu_jellyfin_api_key", "").strip()
            jf_user = settings.get("pu_jellyfin_user_id", "").strip()
            if jf_url and jf_key and _safe_url(jf_url):
                music_clients.append(JellyfinClient(jf_url, jf_key, jf_user))

            # Plex
            plex_url = settings.get("pu_plex_url", "").strip()
            plex_token = settings.get("pu_plex_token", "").strip()
            if plex_url and plex_token and _safe_url(plex_url):
                music_clients.append(PlexClient(plex_url, plex_token))

            # Tautulli
            tautulli_url = settings.get("pu_tautulli_url", "").strip()
            tautulli_key = settings.get("pu_tautulli_api_key", "").strip()
            if tautulli_url and tautulli_key and _safe_url(tautulli_url):
                music_clients.append(TautulliClient(tautulli_url, tautulli_key))

        # Letterboxd
        letterboxd = None
        if settings.get("pu_movies_enabled") == "1":
            lb_rss = settings.get("pu_letterboxd_rss_url", "").strip()
            if lb_rss and _safe_url(lb_rss):
                letterboxd = LetterboxdClient(lb_rss)

        # Goodreads
        goodreads = None
        if settings.get("pu_books_enabled") == "1":
            gr_rss = settings.get("pu_goodreads_rss_url", "").strip()
            if gr_rss and _safe_url(gr_rss):
                goodreads = GoodreadsClient(gr_rss)

        # Audiobookshelf
        audiobookshelf = None
        if settings.get("pu_abs_enabled") == "1":
            abs_url = settings.get("pu_abs_url", "").strip()
            abs_token = settings.get("pu_abs_token", "").strip()
            if abs_url and abs_token and _safe_url(abs_url):
                audiobookshelf = AudiobookshelfClient(abs_url, abs_token)

        return music_clients, letterboxd, goodreads, audiobookshelf

    def _get_mastodon_client(self, settings: dict) -> Mastodon | None:
        instance = settings.get("instance_url")
        token = settings.get("access_token")
        if not instance or not token:
            return None
        return Mastodon(
            access_token=token,
            api_base_url=instance,
            ratelimit_method="wait",  # wait if rate limited rather than crash
        )

    def _load_album_session(self) -> dict | None:
        """Restore album session from DB, converting JSON-safe types back."""
        try:
            with get_db() as conn:
                raw = get_setting(conn, "pu_album_session")
            if not raw:
                return None
            data = json.loads(raw)
            data["tracks_seen"] = {tuple(t) for t in data["tracks_seen"]}
            data["last_track_key"] = tuple(data["last_track_key"])
            return data
        except Exception:
            return None

    def _save_album_session(self) -> None:
        """Persist current album session to DB."""
        try:
            if self._album_session is None:
                with get_db() as conn:
                    set_setting(conn, "pu_album_session", "")
                return
            data = {**self._album_session}
            data["tracks_seen"] = [list(t) for t in data["tracks_seen"]]
            data["last_track_key"] = list(data["last_track_key"])
            with get_db() as conn:
                set_setting(conn, "pu_album_session", json.dumps(data))
        except Exception as e:
            logger.error(f"Failed to save album session: {e}")

    def _post_toot_with_cover(
        self,
        mastodon: Mastodon,
        toot_text: str,
        cover_bytes: bytes | None,
        label: str,
        mime: str = "image/jpeg",
        visibility: str = "public",
    ) -> None:
        """Upload cover (if any) and post a status. Logs errors."""
        media_ids = None
        if cover_bytes:
            try:
                media = mastodon.media_post(
                    BytesIO(cover_bytes),
                    mime_type=mime,
                    description=label,
                )
                media_ids = [media["id"]]
            except MastodonError as e:
                logger.error(f"Failed to upload cover ({label}): {e}")
        try:
            mastodon.status_post(toot_text, media_ids=media_ids, visibility=visibility)
        except MastodonError as e:
            logger.error(f"Failed to post toot ({label}): {e}")

    def _update_profile_fields(self, client: Mastodon, managed_fields: dict[str, str]) -> bool:
        """Update multiple profile fields in a single API call.

        managed_fields: dict of {field_name: value} for fields this tool manages.
        Preserves non-managed fields and respects the configured field order.
        Uses a cached copy of the current fields to avoid an extra API call
        on every invocation; cache is refreshed after each successful update.
        """
        try:
            if self._cached_fields is None:
                account = client.account_verify_credentials()
                self._cached_fields = account.get("fields", [])
            current_fields = self._cached_fields
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
            # Invalidate cache so next update re-fetches the real current state
            self._cached_fields = None
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

            music_clients, letterboxd, goodreads, audiobookshelf = self._build_clients(settings)
            mastodon = self._get_mastodon_client(settings)
            custom_enabled = settings.get("pu_custom_enabled") == "1"

            if not mastodon:
                self.error = "Mastodon not configured"
                self.running = False
                return

            if not music_clients and not letterboxd and not goodreads and not custom_enabled and not audiobookshelf:
                self.error = "No sources enabled"
                self.running = False
                return

            music_interval = int(_s(settings, "pu_music_interval"))
            movie_interval = int(_s(settings, "pu_movie_interval"))
            book_interval = int(_s(settings, "pu_book_interval"))
            abs_interval = max(60, int(_s(settings, "pu_abs_interval")))
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
                            with get_db() as conn:
                                set_setting(conn, "pu_last_track_info", track_info)
                        self.last_music_update = now

                        # Album listen detection — always poll Navidrome directly so albumId is available
                        # (primary music source may be Last.fm which has no albumId)
                        navidrome_client = next(
                            (c for c in music_clients if isinstance(c, NavidromeClient)), None
                        )
                        if settings.get("pu_album_enabled") == "1":
                            nd_track = navidrome_client.get_recent_track() if navidrome_client else None
                            if navidrome_client and nd_track and nd_track.get("albumId") and nd_track.get("now_playing"):
                                album_id = nd_track["albumId"]
                                disc = nd_track.get("discNumber", 1) or 1
                                track_num = nd_track.get("track", 0) or 0
                                track_key = (disc, track_num)

                                if not self._album_session or self._album_session["album_id"] != album_id:
                                    # New album started — fetch metadata and open a new session
                                    album_info = navidrome_client.get_album_info(album_id)
                                    if album_info:
                                        self._album_session = {
                                            "album_id": album_id,
                                            "tracks_seen": {track_key},
                                            "total_tracks": album_info["total_tracks"],
                                            "album_info": album_info,
                                            "posted": False,
                                            "last_track_key": track_key,
                                        }
                                        self._save_album_session()
                                        logger.info(f"Album session started: {album_info['name']} ({album_info['total_tracks']} tracks)")
                                elif not self._album_session["posted"]:
                                    last_key = self._album_session["last_track_key"]
                                    # Reset if playing out of order (going backwards)
                                    if track_key < last_key and track_key != last_key:
                                        logger.info(f"Album session reset (out of order): {self._album_session['album_info']['name']}")
                                        album_info = navidrome_client.get_album_info(album_id)
                                        if album_info:
                                            self._album_session = {
                                                "album_id": album_id,
                                                "tracks_seen": {track_key},
                                                "total_tracks": album_info["total_tracks"],
                                                "album_info": album_info,
                                                "posted": False,
                                                "last_track_key": track_key,
                                            }
                                            self._save_album_session()
                                    else:
                                        self._album_session["tracks_seen"].add(track_key)
                                        self._album_session["last_track_key"] = track_key
                                        self._save_album_session()
                                    total = self._album_session["total_tracks"]
                                    seen = len(self._album_session["tracks_seen"])
                                    if total > 0 and seen / total >= 0.65:
                                        with self._post_lock:
                                            # Re-check under lock — a second thread may have
                                            # already posted while we were waiting
                                            if self._album_session.get("posted"):
                                                continue
                                            album_info = self._album_session["album_info"]
                                            toot_text = _format_album_toot(album_info, settings)
                                            cover_bytes = navidrome_client.get_cover_art_bytes(
                                                album_info.get("cover_art_id", album_id)
                                            )
                                            label = f"{album_info['name']} by {album_info['artist']}"
                                            if settings.get("pu_album_confirm") == "1":
                                                webhook_url = settings.get("discord_webhook_url", "").strip()
                                                if webhook_url:
                                                    token = _queue_pending_toot(
                                                        label, toot_text, cover_bytes,
                                                        "image/jpeg", label,
                                                    )
                                                    _send_discord_confirmation(
                                                        webhook_url, label, toot_text,
                                                        f"{APP_URL}/confirm-toot/{token}",
                                                    )
                                                    logger.info(f"Album toot queued for confirmation: {label}")
                                                else:
                                                    logger.warning("pu_album_confirm is set but discord_webhook_url is empty — posting directly")
                                                    self._post_toot_with_cover(mastodon, toot_text, cover_bytes, label, visibility=settings.get("pu_toot_visibility") or "public")
                                            else:
                                                self._post_toot_with_cover(mastodon, toot_text, cover_bytes, label, visibility=settings.get("pu_toot_visibility") or "public")
                                                logger.info(f"Posted album toot: {label} ({seen}/{total} tracks heard)")
                                            self._album_session["posted"] = True
                                            self._save_album_session()

                        # Navidrome starred track → toot
                        if settings.get("pu_nd_star_toot_enabled") == "1" and navidrome_client:
                            try:
                                starred = navidrome_client.get_starred_songs()
                                starred_ids = {str(s["id"]) for s in starred}
                                with get_db() as conn:
                                    known_raw = get_setting(conn, "pu_nd_starred_ids")
                                    if known_raw is None:
                                        # First run: seed current stars so we only post *new* ones going forward
                                        set_setting(conn, "pu_nd_starred_ids", json.dumps(list(starred_ids)))
                                        logger.info(f"Navidrome star toot: seeded {len(starred_ids)} known tracks (no toots posted)")
                                    else:
                                        known_ids = set(json.loads(known_raw))
                                        new_ids = starred_ids - known_ids
                                        if new_ids:
                                            logger.info(f"Navidrome star toot: {len(new_ids)} new starred track(s)")
                                        for song in starred:
                                            if str(song["id"]) not in new_ids:
                                                continue
                                            toot_text = _format_starred_toot(song, settings)
                                            label = f"{song.get('artist')} - {song.get('title')}"
                                            cover_id = song.get("coverArt") or song.get("albumId")
                                            cover_bytes = None
                                            if cover_id:
                                                cover_bytes = navidrome_client.get_cover_art_bytes(cover_id)
                                            if settings.get("pu_nd_star_confirm") == "1":
                                                webhook_url = settings.get("discord_webhook_url", "").strip()
                                                if webhook_url:
                                                    token = _queue_pending_toot(label, toot_text, cover_bytes, "image/jpeg", label)
                                                    _send_discord_confirmation(webhook_url, label, toot_text, f"{APP_URL}/confirm-toot/{token}")
                                                    logger.info(f"Loved track toot queued for confirmation: {label}")
                                                else:
                                                    logger.warning("pu_nd_star_confirm is set but discord_webhook_url is empty — posting directly")
                                                    self._post_toot_with_cover(mastodon, toot_text, cover_bytes, label, visibility=settings.get("pu_toot_visibility") or "public")
                                            else:
                                                self._post_toot_with_cover(mastodon, toot_text, cover_bytes, label, visibility=settings.get("pu_toot_visibility") or "public")
                                                logger.info(f"Posted starred toot: {label}")
                                        if starred_ids != known_ids:
                                            set_setting(conn, "pu_nd_starred_ids", json.dumps(list(starred_ids)))
                            except Exception as e:
                                logger.error(f"Navidrome star toot check failed: {e}")

                    # Movie update
                    if letterboxd and now - self.last_movie_update >= movie_interval:
                        movie = letterboxd.get_recent_movie()
                        movie_info = self._format_movie(movie, settings)
                        if movie_info != self.last_movie_info:
                            self.last_movie_info = movie_info
                            changed = True
                            logger.info(f"Movie changed: {movie_info}")
                            with get_db() as conn:
                                set_setting(conn, "pu_last_movie_info", movie_info)
                        self.last_movie_update = now

                    # Book update
                    if goodreads and now - self.last_book_update >= book_interval:
                        book = goodreads.get_finished_book()
                        book_info = self._format_book(book, settings)
                        if book_info != self.last_book_info:
                            self.last_book_info = book_info
                            changed = True
                            logger.info(f"Book changed: {book_info}")
                            with get_db() as conn:
                                set_setting(conn, "pu_last_book_info", book_info)

                        # Book event toots (started / finished)
                        post_start = settings.get("pu_books_post_start") == "1"
                        post_finish = settings.get("pu_books_post_finish") == "1"
                        if post_start or post_finish:
                            with get_db() as conn:
                                since_id = get_setting(conn, "pu_last_goodreads_entry_id")
                            events = goodreads.get_book_events(since_entry_id=since_id)
                            # Post in chronological order (oldest first)
                            for event in reversed(events):
                                toot_text = None
                                if event["type"] == "started" and post_start:
                                    toot_text = _format_book_started_toot(event, settings)
                                elif event["type"] == "finished" and post_finish:
                                    toot_text = _format_book_finished_toot(event, settings)
                                if toot_text:
                                    try:
                                        mastodon.status_post(toot_text, visibility=settings.get("pu_toot_visibility") or "public")
                                        logger.info(f"Posted book {event['type']} toot: {event['book_title']}")
                                    except MastodonError as e:
                                        logger.error(f"Failed to post book toot: {e}")
                            # Advance cursor to the newest entry we saw (whether we posted or not)
                            if events:
                                with get_db() as conn:
                                    set_setting(conn, "pu_last_goodreads_entry_id", events[0]["entry_id"])

                        self.last_book_update = now

                    # Weekly top artists toot — posted on configured day/hour if enabled
                    if settings.get("pu_weekly_artists_enabled") == "1" and music_clients:
                        now_dt = datetime.now()
                        weekly_day = int(_s(settings, "pu_weekly_artists_day"))
                        weekly_hour = int(_s(settings, "pu_weekly_artists_hour"))
                        if now_dt.weekday() == weekly_day and now_dt.hour == weekly_hour:
                            today_str = now_dt.strftime("%Y-%m-%d")
                            with get_db() as conn:
                                last_posted = get_setting(conn, "pu_last_weekly_artists_date")
                            if last_posted != today_str:
                                top_artists = _get_top_artists_weekly(music_clients)
                                if top_artists:
                                    toot_text = _format_weekly_artists_toot(top_artists, settings)
                                    try:
                                        mastodon.status_post(toot_text, visibility=settings.get("pu_toot_visibility") or "public")
                                        with get_db() as conn:
                                            set_setting(conn, "pu_last_weekly_artists_date", today_str)
                                        logger.info("Posted weekly top artists toot")
                                    except MastodonError as e:
                                        logger.error(f"Failed to post weekly artists toot: {e}")

                    # Audiobookshelf — toot when a new book is started or finished
                    if audiobookshelf and now - self.last_abs_update >= abs_interval:
                        in_progress = audiobookshelf.get_in_progress_books()
                        with get_db() as conn:
                            raw = get_setting(conn, "pu_abs_tooted_ids")
                            raw_prev = get_setting(conn, "pu_abs_prev_in_progress_ids")
                            raw_finished = get_setting(conn, "pu_abs_finished_tooted_ids")
                        tooted_ids: set[str] = set(json.loads(raw)) if raw else set()
                        prev_ids: set[str] = set(json.loads(raw_prev)) if raw_prev else set()
                        finished_tooted: set[str] = set(json.loads(raw_finished)) if raw_finished else set()
                        current_ids = {
                            item["libraryItemId"]
                            for item in in_progress
                            if item.get("libraryItemId")
                        }

                        # New books started
                        for item in in_progress:
                            item_id = item.get("libraryItemId")
                            if not item_id or item_id in tooted_ids:
                                continue
                            book = audiobookshelf.get_book_metadata(item_id)
                            if not book:
                                tooted_ids.add(item_id)  # skip broken items
                                continue
                            share_url = ""
                            abs_public_url = settings.get("abs_public_url", "").strip()
                            if abs_public_url:
                                expiry_hours = int(settings.get("abs_share_expiry_hours") or 0)
                                slug = audiobookshelf.create_share_link(item_id, expiry_hours)
                                if slug:
                                    share_url = f"{abs_public_url.rstrip('/')}/audiobookshelf/share/{slug}"
                            toot_text = _format_abs_toot(book, settings, share_url)
                            cover_bytes = audiobookshelf.get_cover_bytes(item_id)
                            label = book["title"]
                            if settings.get("pu_abs_confirm") == "1":
                                webhook_url = settings.get("discord_webhook_url", "").strip()
                                if webhook_url:
                                    token = _queue_pending_toot(
                                        label, toot_text, cover_bytes,
                                        "image/jpeg", f"Cover of {label}",
                                    )
                                    _send_discord_confirmation(
                                        webhook_url, label, toot_text,
                                        f"{APP_URL}/confirm-toot/{token}",
                                    )
                                    logger.info(f"ABS started toot queued for confirmation: {label}")
                                else:
                                    logger.warning("pu_abs_confirm is set but discord_webhook_url is empty — posting directly")
                                    self._post_toot_with_cover(mastodon, toot_text, cover_bytes, f"Cover of {label}", visibility=settings.get("pu_toot_visibility") or "public")
                                    logger.info(f"Posted ABS started toot: {label}")
                            else:
                                self._post_toot_with_cover(mastodon, toot_text, cover_bytes, f"Cover of {label}", visibility=settings.get("pu_toot_visibility") or "public")
                                logger.info(f"Posted ABS started toot: {label}")
                            tooted_ids.add(item_id)

                        # Books that just left in-progress — check if finished
                        if settings.get("pu_abs_finished_enabled") == "1" and prev_ids:
                            for item_id in prev_ids - current_ids:
                                if item_id in finished_tooted:
                                    continue
                                progress = audiobookshelf.get_user_progress(item_id)
                                if not progress or not progress.get("isFinished"):
                                    continue
                                book = audiobookshelf.get_book_metadata(item_id)
                                finished_tooted.add(item_id)
                                if not book:
                                    continue
                                share_url = ""
                                abs_public_url = settings.get("abs_public_url", "").strip()
                                if abs_public_url:
                                    expiry_hours = int(settings.get("abs_share_expiry_hours") or 0)
                                    slug = audiobookshelf.create_share_link(item_id, expiry_hours)
                                    if slug:
                                        share_url = f"{abs_public_url.rstrip('/')}/audiobookshelf/share/{slug}"
                                toot_text = _format_abs_finished_toot(book, settings, share_url)
                                cover_bytes = audiobookshelf.get_cover_bytes(item_id)
                                label = book["title"]
                                if settings.get("pu_abs_finished_confirm") == "1":
                                    webhook_url = settings.get("discord_webhook_url", "").strip()
                                    if webhook_url:
                                        from app.config import APP_URL
                                        token = _queue_pending_toot(
                                            label, toot_text, cover_bytes,
                                            "image/jpeg", f"Cover of {label}",
                                        )
                                        _send_discord_confirmation(
                                            webhook_url, label, toot_text,
                                            f"{APP_URL}/confirm-toot/{token}",
                                        )
                                        logger.info(f"ABS finished toot queued for confirmation: {label}")
                                    else:
                                        logger.warning("pu_abs_finished_confirm is set but discord_webhook_url is empty — posting directly")
                                        self._post_toot_with_cover(mastodon, toot_text, cover_bytes, f"Cover of {label}", visibility=settings.get("pu_toot_visibility") or "public")
                                        logger.info(f"Posted ABS finished toot: {label}")
                                else:
                                    self._post_toot_with_cover(mastodon, toot_text, cover_bytes, f"Cover of {label}", visibility=settings.get("pu_toot_visibility") or "public")
                                    logger.info(f"Posted ABS finished toot: {label}")

                        # Persist updated sets (cap at 500 to avoid unbounded growth)
                        with get_db() as conn:
                            set_setting(conn, "pu_abs_tooted_ids", json.dumps(list(tooted_ids)[-500:]))
                            set_setting(conn, "pu_abs_prev_in_progress_ids", json.dumps(list(current_ids)))
                            set_setting(conn, "pu_abs_finished_tooted_ids", json.dumps(list(finished_tooted)[-500:]))
                        self.last_abs_update = now

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

                _expire_pending_toots()
                self._stop_event.wait(loop_interval)

        except Exception as e:
            logger.exception("Profile updater failed to start")
            self.error = str(e)
        finally:
            self.running = False
