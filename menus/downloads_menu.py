import asyncio
import questionary
from utils.logger import log_info, log_warning, log_error
from utils.track_checker import check_downloaded_files, check_downloaded_playlists
from utils.loaders import load_primary_tracks, load_playlists, load_exportify_playlists, load_exportify_tracks
from downloader.base_downloader import download_track, batch_download
from downloader.playlist_download import download_playlist
from downloader.youtube_link_downloader import download_from_link, download_from_playlist
import os


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
        playlists = load_playlists(config["playlists_file"])
        _, pending = check_downloaded_playlists(config["output_dir"], playlists)

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
                questionary.Choice(title=f"{pl['name']} ({len(pl['tracks'])} tracks)", value=pl)
                for pl in pending
            ]
            selected = questionary.checkbox(
                "Select playlists to download (space to toggle, enter to confirm):",
                choices=choices,
            ).ask()
            if not selected:
                log_info("No playlists selected.")
                return
            to_download = selected

        for playlist in to_download:
            asyncio.run(
                download_playlist(
                    playlist["name"],
                    playlist["tracks"],
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
            questionary.Choice(title=f"{pl['name']} ({len(pl['tracks'])} tracks)", value=pl)
            for pl in playlists
        ]
        selected = questionary.checkbox(
            "Select playlists to download (space to toggle, enter to confirm):",
            choices=choices,
        ).ask()

        if not selected:
            log_info("No playlists selected.")
            return

        for playlist in selected:
            # Check which tracks are already downloaded
            _, pending_tracks = check_downloaded_files(config["output_dir"], playlist["tracks"])
            already_downloaded_count = len(playlist["tracks"]) - len(pending_tracks)

            # Show summary before downloading
            log_info(f"Playlist: {playlist['name']}")
            log_info(f"  Already downloaded: {already_downloaded_count}")
            log_info(f"  Pending download: {len(pending_tracks)}")

            if not pending_tracks:
                log_info("✅ All tracks already downloaded. Skipping...")
                continue

            # Ask user if they want to proceed
            confirm = questionary.confirm(
                f"Download {len(pending_tracks)} new tracks from '{playlist['name']}'?"
            ).ask()

            if not confirm:
                log_info(f"❌ Skipped playlist: {playlist['name']}")
                continue

            # Run download only for pending tracks
            asyncio.run(
                download_playlist(
                    playlist["name"],
                    pending_tracks,
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

