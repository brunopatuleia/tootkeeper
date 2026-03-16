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
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import BytesIO
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

    def get_top_artists_weekly(self, limit: int = 5) -> list[dict]:
        params = {
            "method": "user.getTopArtists",
            "user": self.username,
            "api_key": self.api_key,
            "format": "json",
            "period": "7day",
            "limit": limit,
        }
        try:
            resp = requests.get(self.API_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            artists = data.get("topartists", {}).get("artist", [])
            if not isinstance(artists, list):
                artists = [artists]
            return [
                {"name": a.get("name", "Unknown"), "playcount": int(a.get("playcount", 0))}
                for a in artists[:limit]
            ]
        except Exception as e:
            logger.error(f"Last.fm top artists failed: {e}")
            return []


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
                description = entry.get("description", "")

                # Finished / rated book
                rating_match = re.search(r"gave (\d+(?:\.\d+)?) stars? to", description, re.IGNORECASE)
                if rating_match:
                    rating = float(rating_match.group(1))
                    soup = BeautifulSoup(description, "html.parser")
                    title_elem = soup.find("a", class_="bookTitle")
                    author_elem = soup.find("a", class_="authorName")
                    if title_elem:
                        events.append({
                            "type": "finished",
                            "book_title": title_elem.get_text(strip=True),
                            "author": author_elem.get_text(strip=True) if author_elem else "",
                            "rating": rating,
                            "entry_id": entry_id,
                        })
                    continue

                # Started / currently reading
                title_lower = title.lower()
                if "currently-reading" in title_lower or "currently reading" in title_lower or \
                        "started reading" in title_lower:
                    soup = BeautifulSoup(description, "html.parser")
                    title_elem = soup.find("a", class_="bookTitle")
                    author_elem = soup.find("a", class_="authorName")
                    if title_elem:
                        events.append({
                            "type": "started",
                            "book_title": title_elem.get_text(strip=True),
                            "author": author_elem.get_text(strip=True) if author_elem else "",
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
        """Return all books with progress > 0 that are not finished."""
        try:
            resp = requests.get(
                f"{self.server_url}/api/me/media-progress",
                headers=self._headers,
                timeout=10,
            )
            resp.raise_for_status()
            return [
                item for item in resp.json()
                if item.get("mediaItemType") == "book"
                and not item.get("isFinished")
                and item.get("progress", 0) > 0
            ]
        except Exception as e:
            logger.error(f"Audiobookshelf media-progress failed: {e}")
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
                "author": meta.get("authorName") or "",
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
    author_str = f" by {event['author']}" if event.get("author") else ""
    hashtags = settings.get("pu_books_hashtags", "").strip() or "#books #amreading"
    return f"{emoji}Just started reading: {event['book_title']}{author_str}\n\n{hashtags}"


def _format_book_finished_toot(event: dict, settings: dict) -> str:
    emoji = "📚 " if _s(settings, "pu_show_emoji") == "1" else ""
    author_str = f" by {event['author']}" if event.get("author") else ""
    stars = _format_stars(event.get("rating"))
    rating_str = f" — {stars}" if stars else ""
    hashtags = settings.get("pu_books_hashtags", "").strip() or "#books #bookworm"
    return f"{emoji}Just finished reading: {event['book_title']}{author_str}{rating_str}\n\n{hashtags}"


def _format_album_toot(album: dict, settings: dict) -> str:
    """Format a toot for a completed album listen session."""
    artist = album.get("artist", "Unknown Artist")
    name = album.get("name", "Unknown Album")
    year = album.get("year", "")
    genres = album.get("genres", [])

    album_line = f"[{year}] {name}" if year else name

    genre_tags = " ".join(
        "#" + "".join(w.capitalize() for w in g.split())
        for g in genres[:5]
    )

    base_tags = settings.get("pu_album_hashtags", "").strip() or "#NowPlaying"
    hashtags = f"{base_tags} {genre_tags}".strip() if genre_tags else base_tags

    return "\n".join([artist, album_line, "", hashtags])


def _format_abs_toot(book: dict, settings: dict) -> str:
    """Format a toot for a newly started Audiobookshelf book."""
    title = book.get("title", "Unknown")
    author = book.get("author", "")
    year = book.get("year", "")
    genres = book.get("genres", [])

    title_line = f"{title} [{year}]" if year else title

    # Convert genres to hashtags: "Science Fiction" → "#ScienceFiction"
    genre_tags = " ".join(
        "#" + "".join(w.capitalize() for w in g.split())
        for g in genres[:5]
    )

    base_tags = settings.get("pu_abs_hashtags", "").strip() or "#NowReading #Audiobooks #Books"
    hashtags = f"{base_tags} {genre_tags}".strip() if genre_tags else base_tags

    parts = [title_line]
    if author:
        parts.append(author)
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
    "pu_album_hashtags": "#NowPlaying",
}


def _s(settings: dict, key: str) -> str:
    """Get a profile-updater setting with defaults."""
    return settings.get(key) or DEFAULTS.get(key, "")


class ProfileUpdater:
    def __init__(self):
        self.running = False
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
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
        self._album_session: dict | None = None
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
        """Build music clients, letterboxd, goodreads, and audiobookshelf from settings."""
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
            if jf_url and jf_key:
                music_clients.append(JellyfinClient(jf_url, jf_key, jf_user))

            # Plex
            plex_url = settings.get("pu_plex_url", "").strip()
            plex_token = settings.get("pu_plex_token", "").strip()
            if plex_url and plex_token:
                music_clients.append(PlexClient(plex_url, plex_token))

            # Tautulli
            tautulli_url = settings.get("pu_tautulli_url", "").strip()
            tautulli_key = settings.get("pu_tautulli_api_key", "").strip()
            if tautulli_url and tautulli_key:
                music_clients.append(TautulliClient(tautulli_url, tautulli_key))

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

        # Audiobookshelf
        audiobookshelf = None
        if settings.get("pu_abs_enabled") == "1":
            abs_url = settings.get("pu_abs_url", "").strip()
            abs_token = settings.get("pu_abs_token", "").strip()
            if abs_url and abs_token:
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

                        # Album listen detection (Navidrome only — requires albumId + track)
                        if settings.get("pu_album_enabled") == "1" and track and track.get("albumId"):
                            navidrome_client = next(
                                (c for c in music_clients if isinstance(c, NavidromeClient)), None
                            )
                            if navidrome_client:
                                album_id = track["albumId"]
                                track_key = (track.get("discNumber", 1), track.get("track", 0))

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
                                        }
                                        logger.info(f"Album session started: {album_info['name']} ({album_info['total_tracks']} tracks)")
                                elif not self._album_session["posted"]:
                                    self._album_session["tracks_seen"].add(track_key)
                                    total = self._album_session["total_tracks"]
                                    seen = len(self._album_session["tracks_seen"])
                                    if total > 0 and seen / total >= 0.75:
                                        album_info = self._album_session["album_info"]
                                        toot_text = _format_album_toot(album_info, settings)
                                        cover_bytes = navidrome_client.get_cover_art_bytes(
                                            album_info.get("cover_art_id", album_id)
                                        )
                                        media_ids = None
                                        if cover_bytes:
                                            try:
                                                media = mastodon.media_post(
                                                    BytesIO(cover_bytes),
                                                    mime_type="image/jpeg",
                                                    description=f"{album_info['name']} by {album_info['artist']}",
                                                )
                                                media_ids = [media["id"]]
                                            except MastodonError as e:
                                                logger.error(f"Failed to upload album cover: {e}")
                                        try:
                                            mastodon.status_post(toot_text, media_ids=media_ids, visibility="public")
                                            logger.info(f"Posted album toot: {album_info['name']} ({seen}/{total} tracks heard)")
                                            self._album_session["posted"] = True
                                        except MastodonError as e:
                                            logger.error(f"Failed to post album toot: {e}")

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
                                        mastodon.status_post(toot_text, visibility="public")
                                        logger.info(f"Posted book {event['type']} toot: {event['book_title']}")
                                    except MastodonError as e:
                                        logger.error(f"Failed to post book toot: {e}")
                            # Advance cursor to the newest entry we saw (whether we posted or not)
                            if events:
                                with get_db() as conn:
                                    set_setting(conn, "pu_last_goodreads_entry_id", events[0]["entry_id"])

                        self.last_book_update = now

                    # Weekly top artists toot — posted at Monday 00:xx if enabled
                    if settings.get("pu_weekly_artists_enabled") == "1" and music_clients:
                        now_dt = datetime.now()
                        if now_dt.weekday() == 0 and now_dt.hour == 0:
                            today_str = now_dt.strftime("%Y-%m-%d")
                            with get_db() as conn:
                                last_posted = get_setting(conn, "pu_last_weekly_artists_date")
                            if last_posted != today_str:
                                top_artists = _get_top_artists_weekly(music_clients)
                                if top_artists:
                                    toot_text = _format_weekly_artists_toot(top_artists, settings)
                                    try:
                                        mastodon.status_post(toot_text, visibility="public")
                                        with get_db() as conn:
                                            set_setting(conn, "pu_last_weekly_artists_date", today_str)
                                        logger.info("Posted weekly top artists toot")
                                    except MastodonError as e:
                                        logger.error(f"Failed to post weekly artists toot: {e}")

                    # Audiobookshelf — toot when a new book is started
                    if audiobookshelf and now - self.last_abs_update >= abs_interval:
                        in_progress = audiobookshelf.get_in_progress_books()
                        with get_db() as conn:
                            raw = get_setting(conn, "pu_abs_tooted_ids")
                        tooted_ids: set[str] = set(json.loads(raw)) if raw else set()

                        for item in in_progress:
                            item_id = item.get("libraryItemId")
                            if not item_id or item_id in tooted_ids:
                                continue
                            book = audiobookshelf.get_book_metadata(item_id)
                            if not book:
                                tooted_ids.add(item_id)  # skip broken items
                                continue
                            toot_text = _format_abs_toot(book, settings)
                            # Try to attach cover image
                            media_ids = None
                            cover_bytes = audiobookshelf.get_cover_bytes(item_id)
                            if cover_bytes:
                                try:
                                    media = mastodon.media_post(
                                        BytesIO(cover_bytes),
                                        mime_type="image/jpeg",
                                        description=f"Cover of {book['title']}",
                                    )
                                    media_ids = [media["id"]]
                                except MastodonError as e:
                                    logger.error(f"Failed to upload ABS cover ({book['title']}): {e}")
                            try:
                                mastodon.status_post(toot_text, media_ids=media_ids, visibility="public")
                                logger.info(f"Posted ABS toot: {book['title']}")
                            except MastodonError as e:
                                logger.error(f"Failed to post ABS toot ({book['title']}): {e}")
                            tooted_ids.add(item_id)

                        # Persist updated set (cap at 500 to avoid unbounded growth)
                        updated = list(tooted_ids)[-500:]
                        with get_db() as conn:
                            set_setting(conn, "pu_abs_tooted_ids", json.dumps(updated))
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

                self._stop_event.wait(loop_interval)

        except Exception as e:
            logger.exception("Profile updater failed to start")
            self.error = str(e)
        finally:
            self.running = False
