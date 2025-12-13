import asyncio
import questionary
from utils.logger import log_info, log_warning, log_error
from utils.track_checker import (
    check_downloaded_files,
    existing_track_keys_in_dir,
    track_key,
)
from utils.loaders import load_primary_tracks, load_playlists, load_exportify_playlists, load_exportify_tracks
from downloader.base_downloader import download_track, batch_download
from downloader.playlist_download import download_playlist
from downloader.youtube_link_downloader import download_from_link, download_from_playlist
from menus.song_selection_menu import select_songs_for_playlist
import os


def _sanitize_playlist_name(name: str) -> str:
    return (name or "").replace("/", "-").strip()


def _normalize_legacy_playlist_tracks(pl: dict) -> list:
    """Normalize legacy playlists.json playlist items into [{'artist','track'}, ...]."""
    tracks = []
    for item in pl.get("items", []) or []:
        t = item.get("track") if isinstance(item, dict) else None
        if not t:
            continue
        artist = (t.get("artistName") or "").strip()
        name = (t.get("trackName") or "").strip()
        if artist and name:
            tracks.append({"artist": artist, "track": name})
    return tracks


def downloads_menu(config):
    """
    Displays the Downloads menu and handles related actions.
    """
    choice = questionary.select(
        "📥 Downloads Menu — What would you like to do?",
        choices=[
            "Download all pending (sequential) - Works with CSV and JSON sources",
            "Download all pending (batch async) - Works with CSV and JSON sources",
            "Search & Download a single track",
            "Download from Exportify CSV folder",
            "Download from playlists file (legacy Spotify export)",
            "Download from YouTube link/playlist",
            "Back",
        ]
    ).ask()

    if choice == "Download all pending (sequential) - Works with CSV and JSON sources":
        # Get tracks from the appropriate source based on configuration
        tracks = load_primary_tracks(config)
        
        if not tracks:
            log_warning("No tracks found. Please check your configuration and source files.")
            return
            
        _, pending = check_downloaded_files(config["output_dir"], tracks)
        log_info(f"Total tracks: {len(tracks)} | Pending: {len(pending)}")
        
        if not pending:
            log_info("✅ All tracks already downloaded. Nothing to do.")
            return
            
        log_info(f"Downloading {len(pending)} tracks sequentially...")
        for t in pending:
            download_track(
                t["artist"],
                t["track"],
                config["output_dir"],
                config["audio_format"],
                config["sleep_between"],
            )

    elif choice == "Download all pending (batch async) - Works with CSV and JSON sources":
        # Get tracks from the appropriate source based on configuration
        tracks = load_primary_tracks(config)
        
        if not tracks:
            log_warning("No tracks found. Please check your configuration and source files.")
            return
            
        _, pending = check_downloaded_files(config["output_dir"], tracks)
        log_info(f"Total tracks: {len(tracks)} | Pending: {len(pending)}")
        
        if not pending:
            log_info("✅ All tracks already downloaded. Nothing to do.")
            return
            
        log_info(f"Downloading {len(pending)} tracks in batch async mode...")
        asyncio.run(batch_download(pending, config["output_dir"], config["audio_format"]))

    elif choice == "Search & Download a single track":
        # Ask for artist
        artist = questionary.text("Enter artist name (leave empty if unknown):").ask()
        artist = (artist.strip() if artist else "").strip()

        # Ask for song (must exist)
        song = questionary.text("Enter song title (required):").ask()
        song = (song.strip() if song else "").strip()

        # Validate song title
        if not song:
            log_warning("Song title is required. Please try again.")
            return  # go back to menu or prompt again

        # If artist is missing, pick a random fallback
        if not artist:
            artist = "Unknown Artist"
            log_info(f"No artist provided. Using random artist: {artist}")

        # Download track
        download_track(
            artist,
            song,
            config["output_dir"],
            config["audio_format"],
            config["sleep_between"],
        )

    elif choice == "Download from playlists file (legacy Spotify export)":
        raw_playlists = load_playlists(config["playlists_file"])

        # Normalize legacy playlists to flat track lists so we can run song selection.
        playlists = []
        for pl in raw_playlists or []:
            name = (pl.get("name") or "").strip()
            if not name:
                continue
            tracks = _normalize_legacy_playlist_tracks(pl)
            playlists.append({"name": name, "tracks": tracks})

        if not playlists:
            log_info("No playlists found in playlists file.")
            return

        # Pending playlist list is based on missing songs in each playlist destination folder.
        pending = []
        for pl in playlists:
            playlist_dir = os.path.join(config["output_dir"], _sanitize_playlist_name(pl["name"]))
            existing = existing_track_keys_in_dir(playlist_dir)
            missing_count = 0
            for t in pl["tracks"]:
                if track_key(t) not in existing:
                    missing_count += 1
            if missing_count > 0:
                pending.append(pl)

        if not pending:
            log_info("No playlists pending download.")
            return

        sub_choice = questionary.select(
            "Select download mode:",
            choices=["Download ALL pending playlists", "Pick which playlists to download"],
        ).ask()

        to_download = pending if sub_choice == "Download ALL pending playlists" else []

        if sub_choice == "Pick which playlists to download":
            choices = [
                questionary.Choice(title=f"{pl['name']} ({len(pl['tracks'])} tracks)", value=pl["name"])
                for pl in pending
            ]
            selected_names = questionary.checkbox(
                "Select playlists to download (space to toggle, enter to confirm):",
                choices=choices,
            ).ask()
            if not selected_names:
                log_info("No playlists selected.")
                return
            # Map selected names back to playlist objects
            to_download = [pl for pl in pending if pl["name"] in selected_names]

        for playlist in to_download:
            playlist_dir = os.path.join(config["output_dir"], _sanitize_playlist_name(playlist["name"]))
            existing = existing_track_keys_in_dir(playlist_dir)
            exists_count = sum(1 for t in playlist["tracks"] if track_key(t) in existing)

            log_info(f"Playlist: {playlist['name']}")
            log_info(f"  Total tracks: {len(playlist['tracks'])}")
            log_info(f"  Already exists in folder: {exists_count}")

            selected_tracks = select_songs_for_playlist(
                playlist_name=playlist["name"],
                tracks=playlist["tracks"],
                playlist_dir=playlist_dir,
            )

            if not selected_tracks:
                log_info(f"❌ Skipped playlist: {playlist['name']}")
                continue

            confirm = questionary.confirm(
                f"Download {len(selected_tracks)} selected tracks into '{playlist['name']}'?"
            ).ask()
            if not confirm:
                log_info(f"❌ Skipped playlist: {playlist['name']}")
                continue

            asyncio.run(
                download_playlist(
                    playlist["name"],
                    selected_tracks,
                    config["output_dir"],
                    config["audio_format"],
                    config["sleep_between"],
                )
            )

    elif choice == "Download from Exportify CSV folder":
        exportify_dir = config.get("exportify_watch_folder", "data/exportify")
        playlists = load_exportify_playlists(exportify_dir)

        if not playlists:
            log_info("No CSV playlists found in exportify folder.")
            return

        choices = [
            questionary.Choice(title=f"{pl['name']} ({len(pl['tracks'])} tracks)", value=pl["name"])
            for pl in playlists
        ]
        selected_names = questionary.checkbox(
            "Select playlists to download (space to toggle, enter to confirm):",
            choices=choices,
        ).ask()

        if not selected_names:
            log_warning("❌ No playlists selected.")
            log_info("")
            log_info("💡 How to use the checkbox interface:")
            log_info("   • Use ↑↓ arrows to navigate between playlists")
            log_info("   • Press SPACE to select/deselect a playlist")
            log_info("   • Selected items will be marked with ✓")
            log_info("   • Press ENTER when finished to confirm your selection")
            log_info("")
            
            retry = questionary.select(
                "What would you like to do?",
                choices=[
                    "Try selecting playlists again",
                    "Back to Downloads menu"
                ]
            ).ask()
            
            if retry == "Try selecting playlists again":
                # Retry the selection process
                selected_names = questionary.checkbox(
                    "Select playlists to download (space to toggle, enter to confirm):",
                    choices=choices,
                ).ask()
                
                if not selected_names:
                    log_info("No playlists selected. Returning to Downloads menu.")
                    return
            else:
                return

        for name in selected_names:
            playlist = next(pl for pl in playlists if pl["name"] == name)

            # IMPORTANT: playlist downloads go into music/<playlist>/, so existence checks must use that folder.
            playlist_dir = os.path.join(config["output_dir"], _sanitize_playlist_name(playlist["name"]))
            existing = existing_track_keys_in_dir(playlist_dir)
            exists_count = sum(1 for t in playlist["tracks"] if track_key(t) in existing)

            log_info(f"Playlist: {playlist['name']}")
            log_info(f"  Total tracks: {len(playlist['tracks'])}")
            log_info(f"  Already exists in folder: {exists_count}")

            selected_tracks = select_songs_for_playlist(
                playlist_name=playlist["name"],
                tracks=playlist["tracks"],
                playlist_dir=playlist_dir,
            )

            if not selected_tracks:
                log_info(f"❌ Skipped playlist: {playlist['name']}")
                continue

            confirm = questionary.confirm(
                f"Download {len(selected_tracks)} selected tracks into '{playlist['name']}'?"
            ).ask()

            if not confirm:
                log_info(f"❌ Skipped playlist: {playlist['name']}")
                continue

            asyncio.run(
                download_playlist(
                    playlist["name"],
                    selected_tracks,
                    config["output_dir"],
                    config["audio_format"],
                    config["sleep_between"],
                )
            )

    elif choice == "Download from YouTube link/playlist":
        url = questionary.text("Paste YouTube video or playlist URL:").ask()
        if not url:
            log_warning("No URL provided.")
            return

        if "playlist" in url.lower():
            download_from_playlist(
                url, config["output_dir"], config["audio_format"], config["sleep_between"]
            )
        else:
            download_from_link(url, config["output_dir"], config["audio_format"])

