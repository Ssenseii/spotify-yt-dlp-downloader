import os
import json
from config import load_config
from utils.logger import setup_logging, log_info, log_error, log_warning
from menus.main_menu import main_menu
from menus.downloads_menu import downloads_menu
from menus.management_menu import management_menu
from menus.automation_menu import automation_menu
from menus.tools_menu import tools_menu
from menus.config_menu import config_menu

if __name__ == "__main__":
    setup_logging()

    try:
        config = load_config()
    except FileNotFoundError as e:
        log_error(f"Config file not found: {e}")
        log_error("Please create config.json with required settings.")
        exit(1)
    except json.JSONDecodeError as e:
        log_error(f"Config file contains invalid JSON: {e}")
        exit(1)
    except Exception as e:
        log_error(f"Error loading config: {e}")
        exit(1)

    os.makedirs(config["output_dir"], exist_ok=True)

    # CSV-first startup checks (non-fatal)
    if config.get("primary_input_source") == "csv":
        csv_file = config.get("primary_csv_file")
        if csv_file and not os.path.exists(csv_file):
            log_warning(f"primary_csv_file not found: {csv_file} (will fall back to other sources)")
        exportify_dir = config.get("exportify_watch_folder", "data/exportify")
        if exportify_dir and not os.path.exists(exportify_dir):
            # Folder-based CSV inputs are optional, but this is the default location.
            os.makedirs(exportify_dir, exist_ok=True)
            log_info(f"Created exportify_watch_folder: {exportify_dir}")

    # Run startup sync if enabled
    if config.get("auto_sync_enabled", False):
        try:
            from managers.sync_manager import run_sync_once
            log_info("Running startup sync...")
            sync_results = run_sync_once(config)
            if sync_results.get("new_tracks", 0) > 0:
                log_info(f"Startup sync: added {sync_results['new_tracks']} new tracks")
        except Exception as e:
            log_error(f"Startup sync failed: {e}")

    while True:
        choice = main_menu()

        # Downloads Menu
        if choice == "Downloads Menu":
            downloads_menu(config)

        # Management Menu
        elif choice == "Management Menu":
            management_menu(config)

        # Automation Menu
        elif choice == "Automation Menu":
            automation_menu(config)

        # Tools Menu
        elif choice == "Tools Menu":
            tools_menu(config)

        # Config Menu
        elif choice == "Config Menu":
            config = config_menu(config)

        # Exit
        elif choice == "Exit":
            log_info("Exiting program...")
            break

        else:
            log_error("Invalid choice.")
