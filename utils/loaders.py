import os
import csv
from utils.logger import log_info, log_warning, log_error


def _normalize_artists(raw: str) -> str:
    """Normalize Exportify's semicolon-separated Artist Name(s) field to a search-friendly string."""
    raw = (raw or "").strip()
    if not raw:
        return ""

    # Exportify uses semicolons for multi-artist entries, e.g. "A;B;C".
    # Some CSVs may use commas; we only treat commas as delimiters if semicolons are not present.
    delimiter = ";" if ";" in raw else ","
    parts = [p.strip() for p in raw.split(delimiter)]
    parts = [p for p in parts if p]

    if not parts:
        return raw

    # De-dupe while preserving order (some exports can repeat artists).
    seen = set()
    uniq = []
    for p in parts:
        key = p.casefold()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)

    return ", ".join(uniq)


def load_exportify_tracks(csv_file: str):
    """Load a single Exportify CSV into a flat list of track dicts."""
    tracks = []

    if not csv_file or not os.path.exists(csv_file):
        log_warning(f"CSV file not found: {csv_file}")
        return tracks

    try:
        # utf-8-sig handles the BOM that Exportify often includes.
        with open(csv_file, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                artist_raw = (row.get("Artist Name(s)") or row.get("Artist") or "").strip()
                track_name = (row.get("Track Name") or row.get("Track") or "").strip()

                artist = _normalize_artists(artist_raw)

                if not artist or not track_name:
                    continue

                tracks.append(
                    {
                        "artist": artist,
                        "album": (row.get("Album Name") or "").strip(),
                        "track": track_name,
                        "uri": (row.get("Track URI") or "").strip(),
                    }
                )
    except Exception as e:
        log_error(f"Error reading CSV file {csv_file}: {e}")
        return []

    return tracks


def load_primary_tracks(config: dict):
    """Load tracks based on configured primary input source, falling back to tracks_file."""
    primary = (config or {}).get("primary_input_source", "json")

    if primary == "csv":
        csv_file = (config or {}).get("primary_csv_file")
        if csv_file and os.path.exists(csv_file):
            return load_exportify_tracks(csv_file)

        # Fallback: if no explicit CSV file is set, merge all CSVs in exportify_watch_folder.
        exportify_dir = (config or {}).get("exportify_watch_folder", "data/exportify")
        if exportify_dir and os.path.exists(exportify_dir):
            merged = []
            seen = set()
            for filename in sorted(os.listdir(exportify_dir)):
                if not filename.lower().endswith(".csv"):
                    continue
                for t in load_exportify_tracks(os.path.join(exportify_dir, filename)):
                    tid = f"{t.get('artist','').casefold()}|{t.get('track','').casefold()}"
                    if tid in seen:
                        continue
                    seen.add(tid)
                    merged.append(t)
            if merged:
                return merged

    # Fallback: tracks_file can be JSON or CSV.
    return load_tracks((config or {}).get("tracks_file", "data/tracks.json"))


def load_tracks(tracks_file):
    import json

    # Allow tracks_file to be a CSV directly (Exportify format)
    if isinstance(tracks_file, str) and tracks_file.lower().endswith(".csv"):
        return load_exportify_tracks(tracks_file)

    try:
        with open(tracks_file, "r", encoding="utf-8") as f:
            return json.load(f)["tracks"]
    except Exception as e:
        log_error(f"Error loading tracks file: {e}")
        return []


def load_playlists(playlists_file):
    import json
    try:
        with open(playlists_file, "r", encoding="utf-8") as f:
            return json.load(f)["playlists"]
    except Exception as e:
        log_error(f"Error loading playlists file: {e}")
        return []


def load_exportify_playlists(exportify_dir="data/exportify"):
    """
    Scans the exportify folder for CSV files and parses them into playlist dicts.
    Each playlist dict matches your normal playlist structure:
    {
        "name": "Playlist Name",
        "tracks": [
            {"artist": "Artist Name", "track": "Track Name"},
            ...
        ]
    }
    """
    playlists = []
    if not os.path.exists(exportify_dir):
        return playlists

    for file in os.listdir(exportify_dir):
        if not file.lower().endswith(".csv"):
            continue

        playlist_name = os.path.splitext(file)[0]
        playlist_path = os.path.join(exportify_dir, file)

        tracks = []
        try:
            # utf-8-sig handles Exportify BOM
            with open(playlist_path, newline="", encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    artist_raw = (row.get("Artist Name(s)") or row.get("Artist") or "").strip()
                    track = (row.get("Track Name") or row.get("Track") or "").strip()
                    artist = _normalize_artists(artist_raw)

                    if artist and track:
                        tracks.append({"artist": artist, "track": track})
        except Exception as e:
            log_error(f"Error reading CSV file {playlist_path}: {e}")
            continue

        if tracks:
            playlists.append({"name": playlist_name, "tracks": tracks})

    return playlists
