import os
import subprocess
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from utils import log_info, log_success, log_error, log_warning
from tqdm import tqdm


def _get_base_filename(artist: str, track: str) -> str:
    """Generate the base filename used by yt-dlp for consistent metadata embedding."""
    query = f"{artist} - {track}"
    return query.replace("/", "-")


def _embed_metadata_after_download(
    audio_path: str,
    track: dict,
    config: dict,
) -> bool:
    """Embed metadata into downloaded audio file after successful download."""
    try:
        from downloader.metadata import embed_track_metadata
        
        # Check if metadata embedding is enabled
        if not config.get("enable_metadata_embedding", True):
            return True
        
        # Get metadata template and options
        template = config.get("metadata_template", "basic")
        enable_musicbrainz = config.get("enable_musicbrainz_lookup", True)
        
        # Embed metadata
        return embed_track_metadata(
            audio_path,
            track,
            template=template,
            allow_musicbrainz=enable_musicbrainz,
            config=config,
        )
    except ImportError:
        log_warning("Metadata embedding module not available, skipping metadata embedding")
        return True
    except Exception as e:
        log_error(f"Metadata embedding failed for {audio_path}: {e}")
        return False


def download_track(artist, track, output_dir, audio_format, sleep_between, config=None, track_data=None):
    query = f"{artist} - {track}"
    filename = _get_base_filename(artist, track)

    log_info(f"Starting download: {query}")
    cmd = [
        "yt-dlp",
        f"ytsearch1:{query}",
        "-x",
        "--audio-format", audio_format,
        "-o", os.path.join(output_dir, f"{filename}.%(ext)s")
    ]

    try:
        process = subprocess.Popen(cmd)
        process.wait()
        if process.returncode == 0:
            log_success(f"Downloaded successfully: {query}")
            
            # Try to find the downloaded audio file and embed metadata
            try:
                from downloader.metadata import find_downloaded_audio_path
                audio_path = find_downloaded_audio_path(output_dir, filename)
                if audio_path:
                    # Use full track_data if provided, otherwise create minimal dict
                    metadata = track_data if track_data else {"artist": artist, "track": track}
                    metadata_success = _embed_metadata_after_download(audio_path, metadata, config or {})
                    if metadata_success:
                        log_info(f"Metadata embedded for {query}")
                    else:
                        log_warning(f"Metadata embedding failed for {query}")
                else:
                    log_warning(f"Could not find downloaded audio file for metadata embedding: {filename}")
            except Exception as e:
                log_warning(f"Metadata embedding step failed: {e}")
        else:
            log_error(f"Failed to download: {query}")
    except Exception as e:
        log_error(f"Error downloading {query}: {e}")

    time.sleep(sleep_between)


# Worker function for a single track
def _download_worker(track_dict, output_dir, audio_format, config=None):
    artist = track_dict.get("artist", "").strip()
    track = track_dict.get("track", "").strip()
    query = f"{artist} - {track}"
    filename = _get_base_filename(artist, track)

    cmd = [
        "yt-dlp",
        f"ytsearch1:{query}",
        "-x",
        "--audio-format", audio_format,
        "-o", os.path.join(output_dir, f"{filename}.%(ext)s")
    ]

    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            log_success(f"Downloaded: {query}")
            
            # Try to find the downloaded audio file and embed metadata
            try:
                from downloader.metadata import find_downloaded_audio_path
                audio_path = find_downloaded_audio_path(output_dir, filename)
                if audio_path:
                    # Pass the full track dictionary for complete metadata
                    metadata_success = _embed_metadata_after_download(audio_path, track_dict, config or {})
                    if not metadata_success:
                        log_warning(f"Metadata embedding failed for {query}")
                else:
                    log_warning(f"Could not find downloaded audio file for metadata embedding: {filename}")
            except Exception as e:
                log_warning(f"Metadata embedding step failed: {e}")
        else:
            log_error(f"Failed: {query}")
            if stderr:
                log_error(f"Error details: {stderr.strip()}")
    except Exception as e:
        log_error(f"Error downloading {query}: {e}")


# Async batch downloader
async def batch_download(tracks, output_dir, audio_format, max_workers=4, config=None):
    """
    Download multiple tracks concurrently.
    
    Args:
        tracks: List of track dicts with 'artist' and 'track' keys
        output_dir: Output directory for downloads
        audio_format: Audio format (mp3, flac, etc.)
        max_workers: Maximum concurrent downloads
        config: Optional config dict for auto-cleanup and backup
    """
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        tasks = []
        with tqdm(total=len(tracks), desc="Downloading", unit="track") as pbar:
            for track in tracks:
                # Pass the entire track dictionary to preserve all metadata
                task = loop.run_in_executor(executor, _download_worker, track, output_dir, audio_format, config)
                task.add_done_callback(lambda _: pbar.update(1))
                tasks.append(task)
            await asyncio.gather(*tasks)
    
    # Post-download operations if config is provided
    if config:
        # Auto-backup data files
        if config.get("auto_backup", True):
            try:
                from managers.backup_manager import backup_all
                log_info("Creating backups of data files...")
                backup_all(config)
            except ImportError:
                pass
            except Exception as e:
                log_error(f"Backup failed: {e}")
        
        # Auto-cleanup
        if config.get("auto_cleanup", True):
            try:
                from managers.cleanup_manager import cleanup_after_download
                log_info("Running post-download cleanup...")
                cleanup_results = cleanup_after_download(config)
                total_cleaned = (
                    cleanup_results.get("temp_files_removed", 0) +
                    cleanup_results.get("empty_dirs_removed", 0) +
                    cleanup_results.get("partial_files_removed", 0)
                )
                if total_cleaned > 0:
                    log_info(f"Cleaned up {total_cleaned} items")
            except ImportError:
                pass
            except Exception as e:
                log_error(f"Cleanup failed: {e}")
                
        # Auto-metadata embedding for all downloaded files (legacy batch mode)
        if config.get("auto_metadata_embedding", True) and config.get("enable_metadata_embedding", True):
            try:
                from downloader.metadata import embed_metadata
                log_info("Embedding metadata in all downloaded files...")
                embed_metadata(output_dir)
            except ImportError:
                pass
            except Exception as e:
                log_error(f"Auto metadata embedding failed: {e}")
