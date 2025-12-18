from typing import Any, Dict, List, Optional

from .client import SpotifyClient


class SpotifyDataLoader:
    """High-level helpers for extracting playlists and tracks from Spotify.

    Normalized track dicts are designed to be compatible with HARMONI's existing
    loader expectations (see utils/loaders.py):
      - required: artist, track
      - recommended: album, uri, release_date, duration_ms, explicit, popularity

    The song selection UI only relies on {artist, track}, so extra metadata is safe.
    """

    def __init__(self, client: SpotifyClient):
        self.client = client

    def list_all_playlists(self, *, limit: int = 50, max_playlists: Optional[int] = None) -> List[Dict[str, Any]]:
        playlists: List[Dict[str, Any]] = []
        offset = 0

        while True:
            if max_playlists is not None and len(playlists) >= int(max_playlists):
                break

            page = self.client.current_user_playlists(limit=limit, offset=offset)
            items = page.get("items") or []
            for p in items:
                if not isinstance(p, dict):
                    continue
                playlists.append(
                    {
                        "id": p.get("id"),
                        "name": p.get("name"),
                        "tracks_total": (p.get("tracks") or {}).get("total"),
                        "owner": ((p.get("owner") or {}).get("display_name") if isinstance(p.get("owner"), dict) else None),
                        "public": p.get("public"),
                    }
                )

                if max_playlists is not None and len(playlists) >= int(max_playlists):
                    break

            total = page.get("total")
            if total is None:
                break

            offset += len(items)
            if offset >= int(total):
                break

            if not items:
                break

        return playlists

    def load_playlist_tracks(
        self,
        playlist_id: str,
        *,
        limit: int = 100,
        max_tracks: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return playlist tracks as dicts suitable for HARMONI workflows.

        Output dict keys align with Exportify CSV where possible.

        max_tracks:
          Optional safety cap to stop paging after N normalized tracks.
        """

        tracks: List[Dict[str, Any]] = []
        offset = 0

        while True:
            if max_tracks is not None and len(tracks) >= int(max_tracks):
                break

            page = self.client.playlist_items(playlist_id, limit=limit, offset=offset)
            items = page.get("items") or []

            for item in items:
                if not isinstance(item, dict):
                    continue
                track_obj = item.get("track")
                normalized = self._normalize_track(track_obj)
                if normalized:
                    # preserve playlist-added timestamp if available
                    if item.get("added_at"):
                        normalized["added_at"] = item.get("added_at")
                    tracks.append(normalized)

                if max_tracks is not None and len(tracks) >= int(max_tracks):
                    break

            total = page.get("total")
            if total is None:
                break

            offset += len(items)
            if offset >= int(total):
                break

            if not items:
                break

        if max_tracks is not None and len(tracks) > int(max_tracks):
            tracks = tracks[: int(max_tracks)]

        return tracks

    def load_liked_songs(
        self,
        *,
        limit: int = 50,
        max_tracks: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return the user's saved tracks (Liked Songs).

        max_tracks:
          Optional safety cap to stop paging after N normalized tracks.
        """

        tracks: List[Dict[str, Any]] = []
        offset = 0

        while True:
            if max_tracks is not None and len(tracks) >= int(max_tracks):
                break

            page = self.client.current_user_saved_tracks(limit=limit, offset=offset)
            items = page.get("items") or []

            for item in items:
                if not isinstance(item, dict):
                    continue
                track_obj = item.get("track")
                normalized = self._normalize_track(track_obj)
                if normalized:
                    if item.get("added_at"):
                        normalized["added_at"] = item.get("added_at")
                    tracks.append(normalized)

                if max_tracks is not None and len(tracks) >= int(max_tracks):
                    break

            total = page.get("total")
            if total is None:
                break

            offset += len(items)
            if offset >= int(total):
                break

            if not items:
                break

        if max_tracks is not None and len(tracks) > int(max_tracks):
            tracks = tracks[: int(max_tracks)]

        return tracks

    def load_user_playlists_with_tracks(
        self,
        *,
        include_liked_songs: bool = False,
        playlist_limit: int = 50,
        track_limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Load the current user's playlists and attach normalized track lists.

        Returns a list compatible with load_playlists() output:
          [{"name": str, "tracks": [track_dict, ...]}, ...]

        Optionally includes a pseudo-playlist "Liked Songs".
        """

        out: List[Dict[str, Any]] = []

        if include_liked_songs:
            liked = self.load_liked_songs(
                limit=min(50, int(track_limit)),
                max_tracks=int(track_limit),
            )
            out.append({"name": "Liked Songs", "tracks": liked})

        playlists = self.list_all_playlists(limit=min(50, int(playlist_limit)), max_playlists=int(playlist_limit))
        for p in playlists:
            pid = (p.get("id") or "").strip() if isinstance(p, dict) else ""
            name = (p.get("name") or "").strip() if isinstance(p, dict) else ""
            if not pid or not name:
                continue

            tracks = self.load_playlist_tracks(
                pid,
                limit=min(100, int(track_limit)),
                max_tracks=int(track_limit),
            )
            out.append({"name": name, "tracks": tracks})

        return out

    @staticmethod
    def _normalize_artist_list(artists: Any) -> str:
        if not isinstance(artists, list):
            return ""
        names = []
        for a in artists:
            if isinstance(a, dict) and a.get("name"):
                names.append(str(a.get("name")).strip())
        # Match utils.loaders._normalize_artists output style: comma-separated for multi-artist
        names = [n for n in names if n]
        # de-dupe preserving order
        seen = set()
        uniq = []
        for n in names:
            key = n.casefold()
            if key in seen:
                continue
            seen.add(key)
            uniq.append(n)
        return ", ".join(uniq)

    @classmethod
    def _normalize_track(cls, track_obj: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(track_obj, dict):
            return None

        # Episodes / local tracks may appear depending on playlist.
        if track_obj.get("is_local"):
            return None

        artist_name = cls._normalize_artist_list(track_obj.get("artists"))

        album = track_obj.get("album") or {}
        album_name = album.get("name") if isinstance(album, dict) else None
        release_date = album.get("release_date") if isinstance(album, dict) else None

        uri = track_obj.get("uri")
        spotify_id = track_obj.get("id")

        out: Dict[str, Any] = {
            "artist": artist_name or "",
            "track": (track_obj.get("name") or ""),
            "album": album_name or "",
            "uri": uri,
            "spotify_id": spotify_id,
            "duration_ms": track_obj.get("duration_ms"),
            "explicit": track_obj.get("explicit"),
            "popularity": track_obj.get("popularity"),
            "release_date": release_date,
            "isrc": (
                (track_obj.get("external_ids") or {}).get("isrc")
                if isinstance(track_obj.get("external_ids"), dict)
                else None
            ),
            "external_url": (
                (track_obj.get("external_urls") or {}).get("spotify")
                if isinstance(track_obj.get("external_urls"), dict)
                else None
            ),
        }

        # Strip empties (keep numeric/bool values if present)
        cleaned: Dict[str, Any] = {}
        for k, v in out.items():
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            cleaned[k] = v

        # Must have core identifiers for downstream download workflow.
        if not cleaned.get("artist") or not cleaned.get("track"):
            return None

        return cleaned
