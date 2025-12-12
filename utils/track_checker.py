import os
from utils.logger import log_info


def check_downloaded_files(output_dir, tracks):
    """Return (downloaded_count, pending_tracks) based on existing files in output_dir."""
    os.makedirs(output_dir, exist_ok=True)

    try:
        existing_files = set(os.listdir(output_dir))
    except Exception:
        existing_files = set()

    downloaded = []
    pending = []

    for track in tracks or []:
        artist = (track.get("artist") or "").strip()
        name = (track.get("track") or "").strip()
        if not artist or not name:
            continue

        filename = f"{artist} - {name}.mp3".replace("/", "-")
        if filename in existing_files:
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

def check_downloaded_playlists(output_dir, playlists):
    downloaded_playlists = []
    pending_playlists = []

    for pl in playlists or []:
        playlist_name = pl.get("name", "").strip()
        if not playlist_name:
            continue

        sanitized_name = playlist_name.replace("/", "-").strip()
        playlist_dir = os.path.join(output_dir, sanitized_name)

        # Exportify CSV playlists already provide a flat tracks list.
        if pl.get("tracks") and isinstance(pl.get("tracks"), list):
            tracks = [
                {"artist": (t.get("artist") or "").strip(), "track": (t.get("track") or "").strip()}
                for t in pl.get("tracks", [])
                if isinstance(t, dict)
            ]
            tracks = [t for t in tracks if t["artist"] and t["track"]]
        else:
            # Spotify export playlists provide nested items[].track.{artistName, trackName}
            tracks = [
                {"artist": item["track"]["artistName"], "track": item["track"]["trackName"]}
                for item in pl.get("items", [])
                if item.get("track") and item["track"].get("artistName") and item["track"].get("trackName")
            ]

        if not os.path.exists(playlist_dir):
            log_info(f"Playlist folder missing: {playlist_name}")
            pending_playlists.append({"name": playlist_name, "tracks": tracks})
            continue

        try:
            existing_files = set(os.listdir(playlist_dir))
        except Exception:
            existing_files = set()

        downloaded_tracks = []
        pending_tracks = []

        for track in tracks:
            filename = f"{track['artist']} - {track['track']}.mp3".replace("/", "-")
            if filename in existing_files:
                downloaded_tracks.append(track)
            else:
                pending_tracks.append(track)

        log_info(f"{playlist_name} → Downloaded: {len(downloaded_tracks)}, Pending: {len(pending_tracks)}")

        if pending_tracks:
            pending_playlists.append({"name": playlist_name, "tracks": pending_tracks})
        else:
            downloaded_playlists.append({"name": playlist_name, "tracks": downloaded_tracks})

    return downloaded_playlists, pending_playlists