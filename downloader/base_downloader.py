import os
import subprocess
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from utils import log_info, log_success, log_error
from tqdm import tqdm

def download_track(artist, track, output_dir, audio_format, sleep_between):
    query = f"{artist} - {track}"
    filename = query.replace("/", "-")

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
        else:
            log_error(f"Failed to download: {query}")
    except Exception as e:
        log_error(f"Error downloading {query}: {e}")

    time.sleep(sleep_between)


# Worker function for a single track
def _download_worker(artist, track, output_dir, audio_format):
    query = f"{artist} - {track}"
    filename = query.replace("/", "-")

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
                artist = track["artist"].strip()
                song = track["track"].strip()
                task = loop.run_in_executor(executor, _download_worker, artist, song, output_dir, audio_format)
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
