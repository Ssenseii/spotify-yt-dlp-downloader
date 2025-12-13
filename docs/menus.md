# Menus Module Documentation

The `menus/` folder contains all interactive CLI menu implementations. These menus provide the user interface for navigating different features of the application.

## Folder Overview

This module provides the complete command-line interface using the `questionary` library. It organizes functionality into logical menu groups:

- **Main Menu**: Entry point and navigation hub
- **Downloads Menu**: All download-related operations
- **Management Menu**: File management and metadata operations
- **Automation Menu**: Scheduled downloads and resume functionality
- **Tools Menu**: Utility tools and system checks
- **Config Menu**: Configuration management and settings

---

## Files

### `main_menu.py`

**Purpose**: Displays the main navigation menu and returns user selection.

**How it works**:

1. Uses `questionary.select` to create an interactive menu
2. Displays welcome message: "🎵 Welcome to POTTY Spotify & Music Downloader"
3. Presents six options:
   - Downloads Menu
   - Management Menu
   - Automation Menu
   - Tools Menu
   - Config Menu
   - Exit
4. Returns the selected choice as a string

**Key Functions**:

- `main_menu()` - Displays menu and returns user choice

**Dependencies**:

- `questionary` - Interactive CLI prompts

**Usage**: Called from `main.py` in the main loop to get user navigation choice.

---

### `downloads_menu.py`

**Purpose**: Provides all download-related functionality through an interactive menu.

**How it works**:

1. **Menu Display**: Shows download options:

   - Download all pending (sequential)
   - Download all pending (batch async)
   - Search & Download a single track
   - Download from playlists file
   - Download from Exportify CSVs
   - Download from YouTube link/playlist
   - Back

2. **Sequential Downloads**:

   - Loads tracks from the configured primary source
   - Checks which tracks are already downloaded
   - Downloads pending tracks one by one using `download_track`

3. **Batch Downloads**:

   - Same as sequential but uses `batch_download` for concurrent downloads
   - More efficient for large track lists

4. **Single Track Download**:

   - Prompts for artist name (optional)
   - Prompts for song title (required)
   - Uses "Unknown Artist" if artist not provided
   - Downloads immediately

5. **Playlist Downloads (Legacy playlists file)**:

   - Loads playlists from `playlists_file`
   - Lets the user download all pending playlists or pick specific playlists
   - **NEW: Song selection step** (per selected playlist):
     - Lists all songs in the playlist
     - Provides checkbox selection with **Select All / Deselect All** actions
     - Songs that already exist in the destination folder `music/<playlist>/` start unchecked
     - User confirms and downloads only the selected songs

6. **Exportify CSV Downloads**:

   - Scans `data/exportify/` folder for CSV files
   - Parses CSV files into playlist format
   - Shows track counts per playlist
   - Allows multi-select of playlists
   - **NEW: Song selection step** (per selected playlist):
     - Lists all songs in the playlist
     - Provides checkbox selection with **Select All / Deselect All** actions
     - Songs that already exist in the destination folder `music/<playlist>/` start unchecked
     - User confirms and downloads only the selected songs

7. **YouTube Link Downloads**:
   - Prompts for YouTube URL
   - Detects if URL is a playlist or single video
   - Routes to appropriate download function
   - Shows preview and confirmation before downloading

**Key Features**:

- Comprehensive download options
- Duplicate detection before downloading
- User-friendly selection interfaces
- Progress feedback and logging

**Dependencies**:

- `asyncio` - Async operations
- `questionary` - Interactive menus
- `utils.logger` - Logging
- `utils.track_checker` - Existing file detection helpers
- `utils.loaders` - Load tracks/playlists
- `downloader.base_downloader` - Core download functions
- `downloader.playlist_download` - Playlist downloads
- `downloader.youtube_link_downloader` - YouTube downloads

**Usage**: Called from `main.py` when user selects "Downloads Menu".

---

### `song_selection_menu.py`

**Purpose**: Provides the per-playlist song selection UI used by playlist download workflows.

**How it works**:

- Shows a `questionary.checkbox` list of songs in the playlist.
- Adds action rows at the top:
  - **Select All**
  - **Deselect All**
- Determines which songs already exist by scanning the destination folder `music/<playlist>/`.
  - Existing songs are shown with a `(exists)` suffix and start unchecked.
- Returns the selected songs as a normalized list of `{ "artist": str, "track": str }` dicts.

**Usage**: Called by `downloads_menu.py` after playlist selection and before downloading.

---

### `management_menu.py`

**Purpose**: Provides file management and metadata operations.

**How it works**:

1. **Menu Display**: Shows management options:

   - Retry failed downloads
   - Detect duplicates
   - Organize files by artist/album
   - Embed metadata in MP3s
   - Back

2. **Retry Failed Downloads**:

   - Calls `retry_failed` from `downloader.retry_manager`
   - Attempts to re-download tracks that previously failed

3. **Detect Duplicates**:

   - Calls `detect_duplicates` from `managers.file_manager`
   - Scans output directory for duplicate files using MD5 hashing

4. **Organize Files**:

   - Calls `organize_files` from `managers.file_manager`
   - Moves MP3 files into artist-named subdirectories

5. **Embed Metadata**:
   - Calls `embed_metadata` from `downloader.metadata`
   - Adds ID3 tags to MP3 files based on filenames

**Key Features**:

- File organization utilities
- Metadata management
- Duplicate detection
- Failed download recovery

**Dependencies**:

- `questionary` - Interactive menus
- `downloader.retry_manager` - Retry functionality
- `managers.file_manager` - File operations
- `downloader.metadata` - Metadata embedding

**Usage**: Called from `main.py` when user selects "Management Menu".

---

### `automation_menu.py`

**Purpose**: Provides automation features like scheduled downloads and resume functionality.

**How it works**:

1. **Menu Display**: Shows automation options:

   - Resume paused batch download
   - Schedule a download job
   - Back

2. **Resume Batch Download**:

   - Calls `resume_batch` from `managers.resume_manager`
   - Loads saved progress from `data/download_progress.json`
   - Continues downloading from where it left off

3. **Schedule Download**:

   - Calls `schedule_download` from `managers.schedule_manager`
   - Prompts for time in HH:MM format (24-hour)
   - Sets up daily scheduled download job
   - Runs continuously until interrupted

**Key Features**:

- Resume interrupted downloads
- Scheduled automatic downloads
- Progress persistence

**Dependencies**:

- `questionary` - Interactive menus
- `managers.resume_manager` - Resume functionality
- `managers.schedule_manager` - Scheduling

**Usage**: Called from `main.py` when user selects "Automation Menu".

---

### `tools_menu.py`

**Purpose**: Provides access to utility tools and system checks.

**How it works**:

1. **Menu Display**: Shows tool options:

   - System check
   - Library cleanup from broken files
   - Playlist to track list
   - Dependency check
   - Library export as JSON
   - Compress music
   - Choose audio format
   - Open log
   - Help
   - Back

2. **System Check**:

   - Calls `system_check` from `utils.system`
   - Displays CPU, RAM, and disk usage

3. **Library Cleanup**:

   - Calls `library_cleanup` from `tools.library_cleanup`
   - Removes corrupted or broken audio files

4. **Playlist to Track List**:

   - Calls `playlist_to_tracklist` from `tools.playlist_to_tracklist`
   - Converts playlists.json to flat track list format

5. **Dependency Check**:

   - Calls `dependency_check` from `tools.dependency_check`
   - Verifies all required dependencies are installed

6. **Library Export**:

   - Calls `library_export_json` from `tools.library_export_json`
   - Exports all music files in library as JSON

7. **Compress Music**:

   - Calls `compress_music` from `tools.compress_music`
   - Compresses audio files to selected bitrate

8. **Choose Audio Format**:

   - Calls `choose_audio_format` from `tools.choose_audio_format`
   - Updates default audio format in config.json

9. **Open Log**:

   - Calls `open_log` from `tools.open_log`
   - Opens app.log file in default system viewer

10. **Help**:
    - Displays helpful information about the application

**Key Features**:

- Comprehensive utility tools
- System diagnostics
- Library management
- Configuration tools

**Dependencies**:

- `questionary` - Interactive menus
- `utils.logger` - Logging
- `utils.system` - System checks
- All tool modules from `tools/`

**Usage**: Called from `main.py` when user selects "Tools Menu".

---

### `config_menu.py`

**Purpose**: Provides comprehensive configuration management through an interactive menu.

**How it works**:

1. **Menu Display**: Shows configuration options:

   - View current config
   - Update a setting
   - Switch config profile
   - Toggle automation features
   - Reset to defaults
   - Validate configuration
   - Back

2. **View Current Config**:

   - Displays all configuration settings grouped by category:
     - File Paths: tracks_file, playlists_file, output_dir, exportify_watch_folder
     - Download Settings: audio_format, sleep_between, average_download_time
     - Retry Settings: retry_attempts, retry_delay
     - Automation: auto_cleanup, auto_backup, max_backups, auto_sync_enabled, auto_sync_interval
     - Profile: current profile name
   - Formats boolean values as "✓ Enabled" or "✗ Disabled"

3. **Update a Setting**:

   - Lists all editable settings from `CONFIG_SCHEMA`
   - Shows current value for selected setting
   - Handles different input types:
     - **Choices**: Dropdown selection for fields with predefined options
     - **Boolean**: Yes/No confirmation prompt
     - **Numeric**: Text input with min/max validation
     - **String**: Free text input
   - Validates the new value before saving
   - Updates config file and returns updated config dict

4. **Switch Config Profile**:

   - Lists all available profiles (light, advanced, minimal)
   - Shows settings for each profile
   - Indicates current active profile
   - Confirms before applying profile changes
   - Applies profile settings and updates config file
   - Reloads config to reflect changes

5. **Toggle Automation Features**:

   - Quick toggle menu for automation settings:
     - Auto-cleanup after downloads
     - Auto-backup JSON files
     - Auto-sync exportify folder
   - Shows current status (✓ enabled / ✗ disabled)
   - Toggles setting on/off with immediate feedback
   - Updates config file automatically

6. **Reset to Defaults**:

   - Confirms before resetting (requires explicit confirmation)
   - Resets all settings to `DEFAULT_CONFIG` values
   - Reloads config after reset

7. **Validate Configuration**:
   - Validates current configuration against schema
   - Shows validation status (valid ✓ or errors)
   - Displays detailed error messages if validation fails
   - Checks required fields, types, ranges, and choices

**Key Features**:

- Interactive configuration management
- Profile-based configuration switching
- Real-time validation
- User-friendly display with status indicators
- Safe defaults and error handling

**Dependencies**:

- `questionary` - Interactive menus
- `config` - Configuration loading, validation, and profile management
- `utils.logger` - Logging functions

**Usage**: Called from `main.py` when user selects "Config Menu" (if available in main menu).

---

### `__init__.py`

**Purpose**: Makes the menus folder a Python package.

**How it works**: Empty or contains package-level imports/exports.

**Usage**: Allows importing modules from the menus package.
