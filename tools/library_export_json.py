import os
import json
from datetime import datetime
from utils.logger import log_info, log_success, log_error
from constants import VALID_AUDIO_EXTENSIONS

def library_export_json(config):
    """
    Scans the music folder (and all subfolders) for audio files
    and exports the list as harmoni_export_<date>.json inside export/.
    """
    try:
        music_dir = config.get("output_dir", "music")

        if not os.path.exists(music_dir):
            log_error(f"Music folder not found: {music_dir}")
            return

        music_files = []
        for root, _, files in os.walk(music_dir):
            for file in files:
                ext = os.path.splitext(file)[1].lower()
                if ext in VALID_AUDIO_EXTENSIONS:
                    # Keep only the file name
                    music_files.append(file)

        if not music_files:
            log_info("No music files found in the library.")
            return

        export_dir = "export"
        os.makedirs(export_dir, exist_ok=True)

        date_str = datetime.now().strftime("%Y-%m-%d")
        export_path = os.path.join(export_dir, f"harmoni_export_{date_str}.json")

        with open(export_path, "w", encoding="utf-8") as f:
            json.dump({"music_files": music_files}, f, indent=2, ensure_ascii=False)

        log_success(f"Exported {len(music_files)} music files to {export_path}")

    except Exception as e:
        log_error(f"Failed to export music library: {e}")
