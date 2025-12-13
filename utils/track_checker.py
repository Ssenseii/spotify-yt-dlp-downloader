import os
from utils.logger import log_info
from constants import VALID_AUDIO_EXTENSIONS


def _sanitize_playlist_name(playlist_name: str) -> str:
    return (playlist_name or "").replace("/", "-").strip()


def track_key(track: dict) -> str:
    """Canonical identifier for a track: artist.casefold() + '|' + track.casefold()."""
    artist = (track.get("artist") or "").strip()
    name = (track.get("track") or "").strip()
    return f"{artist.casefold()}|{name.casefold()}"


def parse_track_filename(filename: str):
    """Parse 'Artist - Track.ext' into (artist, track). Returns None if not parseable."""
    base = os.path.basename(filename)
    root, ext = os.path.splitext(base)

    if ext.lower() not in VALID_AUDIO_EXTENSIONS:
        return None

    if " - " not in root:
        return None

    try:
        artist, title = root.split(" - ", 1)
    except ValueError:
        return None

    artist = artist.strip()
    title = title.strip()
    if not artist or not title:
        return None

    return artist, title


def existing_track_keys_in_dir(dir_path: str) -> set:
    """Return a set of canonical track keys found in dir_path (non-recursive)."""
    try:
        entries = os.listdir(dir_path)
    except Exception:
        return set()

    keys = set()
    for entry in entries:
        parsed = parse_track_filename(entry)
        if not parsed:
            continue
        artist, title = parsed
        keys.add(f"{artist.casefold()}|{title.casefold()}")

    return keys


def check_downloaded_files(output_dir, tracks):
    """Return (downloaded_count, pending_tracks) based on existing audio files in output_dir.

    Note: This is non-recursive and extension-agnostic.
    """
    os.makedirs(output_dir, exist_ok=True)

    existing_keys = existing_track_keys_in_dir(output_dir)

    downloaded = []
    pending = []

    for track in tracks or []:
        artist = (track.get("artist") or "").strip()
        name = (track.get("track") or "").strip()
        if not artist or not name:
            continue

        if track_key({"artist": artist, "track": name}) in existing_keys:
            downloaded.append(track)
        else:
            pending.append(track)

    log_info(f"Downloaded: {len(downloaded)} tracks, Pending: {len(pending)} tracks")
    return len(downloaded), pending


"""
Checks which playlists and tracks have already been downloaded.
Returns:
    downloaded_playlists: list of dicts with playlist info and downloaded tracks
    pending_playlists: list of dicts with playlist info and pending tracks
"""


def _normalize_playlist_tracks(pl: dict):
    """Normalize both Exportify and legacy playlist formats to [{'artist','track'}, ...]."""
    # Exportify CSV playlists already provide a flat tracks list.
    if pl.get("tracks") and isinstance(pl.get("tracks"), list):
        tracks = [
            {"artist": (t.get("artist") or "").strip(), "track": (t.get("track") or "").strip()}
            for t in pl.get("tracks", [])
            if isinstance(t, dict)
        ]
        return [t for t in tracks if t["artist"] and t["track"]]

    # Spotify export playlists provide nested items[].track.{artistName, trackName}
    tracks = [
        {"artist": item["track"]["artistName"], "track": item["track"]["trackName"]}
        for item in pl.get("items", [])
        if item.get("track") and item["track"].get("artistName") and item["track"].get("trackName")
    ]
    return [{"artist": t["artist"].strip(), "track": t["track"].strip()} for t in tracks if t["artist"].strip() and t["track"].strip()]


def check_downloaded_playlists(output_dir, playlists):
    downloaded_playlists = []
    pending_playlists = []

    for pl in playlists or []:
        playlist_name = (pl.get("name") or "").strip()
        if not playlist_name:
            continue

        sanitized_name = _sanitize_playlist_name(playlist_name)
        playlist_dir = os.path.join(output_dir, sanitized_name)

        tracks = _normalize_playlist_tracks(pl)

        if not os.path.exists(playlist_dir):
            log_info(f"Playlist folder missing: {playlist_name}")
            pending_playlists.append({"name": playlist_name, "tracks": tracks})
            continue

        existing_keys = existing_track_keys_in_dir(playlist_dir)

        downloaded_tracks = []
        pending_tracks = []

        for track in tracks:
            if track_key(track) in existing_keys:
                downloaded_tracks.append(track)
            else:
                pending_tracks.append(track)

        log_info(f"{playlist_name} → Downloaded: {len(downloaded_tracks)}, Pending: {len(pending_tracks)}")

        if pending_tracks:
            pending_playlists.append({"name": playlist_name, "tracks": pending_tracks})
        else:
            downloaded_playlists.append({"name": playlist_name, "tracks": downloaded_tracks})

    return downloaded_playlists, pending_playlists