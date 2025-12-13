import questionary

from utils.logger import log_info, log_warning
from utils.track_checker import existing_track_keys_in_dir, track_key


def select_songs_for_playlist(playlist_name: str, tracks: list, playlist_dir: str) -> list:
    """Let the user select which songs to download for a given playlist.

    - Shows all songs in the playlist.
    - Songs that already exist in playlist_dir start unchecked.

    Returns a list of normalized track dicts: [{'artist': str, 'track': str}, ...]
    """

    tracks = tracks or []

    # Normalize incoming tracks to the downloader-required shape.
    normalized = []
    for t in tracks:
        if not isinstance(t, dict):
            continue
        artist = (t.get("artist") or "").strip()
        name = (t.get("track") or "").strip()
        if not artist or not name:
            continue
        normalized.append({"artist": artist, "track": name})

    if not normalized:
        log_warning(f"Playlist '{playlist_name}' has no valid tracks.")
        return []

    existing_keys = existing_track_keys_in_dir(playlist_dir)

    # Maintain a working set of selected song keys.
    selected_keys = {
        track_key(t)
        for t in normalized
        if track_key(t) not in existing_keys
    }

    while True:
        choices = []

        for t in normalized:
            key = track_key(t)
            exists = key in existing_keys
            label = f"{t['artist']} - {t['track']}" + (" (exists)" if exists else "")
            choices.append(
                questionary.Choice(
                    title=label,
                    value=key,
                    checked=(key in selected_keys),
                )
            )

        selected = questionary.checkbox(
            f"Select songs for playlist: {playlist_name} (space toggles, enter confirms):",
            choices=choices,
        ).ask()

        if not selected:
            log_warning("❌ No songs selected.")
            log_info("")
            log_info("💡 How to use the checkbox interface:")
            log_info("   • Use ↑↓ arrows to navigate")
            log_info("   • Press SPACE to select/deselect")
            log_info("   • Press ENTER to confirm")
            log_info("")

            retry = questionary.select(
                "What would you like to do?",
                choices=["Try selecting songs again", "Skip this playlist"],
            ).ask()

            if retry == "Try selecting songs again":
                continue

            return []

        # Final selection: map keys back to track dicts.
        selected_keys = set(selected)
        return [t for t in normalized if track_key(t) in selected_keys]
