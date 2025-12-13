# Utils Module Documentation

The `utils/` folder contains utility functions used throughout the application, including logging, data loading, system checks, and track verification.

## Folder Overview

This module provides foundational utilities that support all other modules:
- **Logging**: Colored terminal output and file logging
- **Loaders**: JSON and CSV data loading functions
- **System**: System resource monitoring
- **Track Checker**: Verification of downloaded files

---

## Files

### `logger.py`
**Purpose**: Provides logging functionality with colored terminal output and persistent file logging.

**How it works**:

1. **`setup_logging()`**:
   - Configures Python's `logging` module
   - Sets log file to `app.log` (from constants)
   - Sets log level to INFO
   - Configures format: `"%(asctime)s - %(levelname)s - %(message)s"`
   - Initializes `colorama` for cross-platform colored output

2. **`log_info(msg)`**:
   - Prints message in CYAN color to terminal
   - Logs message at INFO level to file

3. **`log_success(msg)`**:
   - Prints message in GREEN color to terminal
   - Logs message at INFO level to file

4. **`log_error(msg)`**:
   - Prints message in RED color to terminal
   - Logs message at ERROR level to file

5. **`log_warning(msg)`**:
   - Prints message in YELLOW color to terminal
   - Logs message at WARNING level to file

**Key Features**:
- Color-coded terminal output for better readability
- Persistent logging to `app.log` file
- Consistent logging format across application
- Cross-platform color support via colorama

**Dependencies**:
- `logging` - Python logging module
- `colorama` - Colored terminal output
- `constants.LOG_FILE` - Log file path

**Usage**: Imported by virtually every module in the application for consistent logging.

---

### `loaders.py`
**Purpose**: Provides functions to load tracks, playlists, and Exportify CSV files.

**How it works**:

1. **`load_tracks(tracks_file)`**:
   - Opens and reads JSON file from `tracks_file` path
   - Parses JSON and extracts `"tracks"` array
   - Returns list of track dictionaries
   - Handles file errors gracefully, returns empty list on failure
   - Logs errors if file cannot be loaded

2. **`load_playlists(playlists_file)`**:
   - Opens and reads JSON file from `playlists_file` path
   - Parses JSON and extracts `"playlists"` array
   - Returns list of playlist dictionaries
   - Handles file errors gracefully, returns empty list on failure
   - Logs errors if file cannot be loaded

3. **`load_exportify_playlists(exportify_dir="data/exportify")`**:
   - Scans directory for CSV files
   - Parses each CSV file using `csv.DictReader`
   - Extracts "Artist Name(s)" and "Track Name" columns
   - Creates playlist dictionary structure matching internal format:
     ```python
     {
       "name": "Playlist Name",
       "tracks": [
         {"artist": "Artist Name", "track": "Track Name"},
         ...
       ]
     }
     ```
   - Returns list of playlist dictionaries
   - Skips non-CSV files
   - Uses filename (without extension) as playlist name

**Key Features**:
- Consistent error handling
- Support for multiple data formats (JSON, CSV)
- Exportify CSV format compatibility
- Graceful degradation on errors

**Dependencies**:
- `os` - File system operations
- `csv` - CSV file parsing
- `json` - JSON file parsing
- `utils.logger` - Logging functions

**Usage**: 
- Called from menu handlers to load data
- Used throughout application when track/playlist data is needed

**Data Formats**:
- **Tracks JSON**: `{"tracks": [{"artist": "...", "album": "...", "track": "...", "uri": "..."}]}`
- **Playlists JSON**: `{"playlists": [{"name": "...", "items": [...]}]}`
- **Exportify CSV**: Columns include "Artist Name(s)" and "Track Name"

---

### `system.py`
**Purpose**: Provides system resource monitoring and health checks.

**How it works**:

1. **`system_check()`**:
   - Uses `psutil` library to gather system metrics:
     - **CPU Usage**: Percentage of CPU currently in use
     - **RAM Available**: Available memory in MB
     - **Disk Free**: Free disk space in GB
   - Displays metrics using logging functions
   - Issues warnings if resources are low:
     - RAM < 200 MB: Warns downloads might fail
     - Disk < 1 GB: Warns low disk space
   - Handles errors gracefully (logs warning if check fails)

**Key Features**:
- Real-time system resource monitoring
- Automatic warnings for low resources
- Cross-platform support via psutil
- User-friendly unit conversions (MB, GB)

**Dependencies**:
- `psutil` - System and process utilities
- `utils.logger` - Logging functions

**Usage**: 
- Called from `menus.tools_menu` for system diagnostics
- Can be used before large batch downloads to verify system readiness

---

### `track_checker.py`
**Purpose**: Verifies which tracks have already been downloaded and which are pending.

**How it works**:

1. **Canonical track identity**:

   - Tracks are identified using a canonical key:
     - `artist.casefold() + "|" + track.casefold()`

2. **Directory scanning is extension-agnostic**:

   - Existing files are discovered by scanning the target directory for any extension in `constants.VALID_AUDIO_EXTENSIONS`.
   - Filenames are parsed using the pattern: `Artist - Track.ext`.

3. **`check_downloaded_files(output_dir, tracks)`**:

   - Scans `output_dir` (non-recursive) for existing audio files
   - For each input track, checks whether its canonical key exists in the directory
   - Returns tuple: `(downloaded_count, pending_list)`
   - Logs summary of downloaded vs pending counts

4. **`check_downloaded_playlists(output_dir, playlists)`**:

   - Normalizes playlist structures to flat `{artist, track}` lists
   - Checks existence against each playlist's destination folder:
     - `output_dir/<playlist_name>/`
   - Returns tuple: `(downloaded_playlists, pending_playlists)`

**Key Features**:
- Efficient file existence checking
- Playlist-folder-aware checking (destination folder based)
- Handles missing playlist folders gracefully
- Works across multiple audio formats (not hardcoded to `.mp3`)

**Dependencies**:
- `os` - File system operations
- `constants.VALID_AUDIO_EXTENSIONS` - Valid audio extensions
- `utils.logger` - Logging functions

**Usage**: 
- Called from `menus.downloads_menu` to avoid duplicates and show accurate counts
- Used by the per-playlist song selection UI to auto-uncheck existing songs

---

### `__init__.py`
**Purpose**: Makes the utils folder a Python package.

**How it works**: May contain package-level imports/exports for convenience.

**Usage**: Allows importing modules from the utils package.

