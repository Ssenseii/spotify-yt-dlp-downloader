# Tools Module Documentation

The `tools/` folder contains utility tools for library management, format conversion, dependency checking, and other helper functions.

## Folder Overview

This module provides standalone utility tools that enhance the application's functionality:
- **Audio Format Management**: Choose and configure audio formats
- **Library Management**: Cleanup, export, and compression
- **Data Conversion**: Playlist to tracklist conversion
- **System Utilities**: Dependency checking and log viewing

---

## Files

### `choose_audio_format.py`
**Purpose**: Allows users to select and set the default audio format for downloads.

**How it works**:

1. **`choose_audio_format(config)`**:
   - Reads current audio format from config
   - Gets list of valid audio extensions from constants
   - Creates display list showing current format as "(active)"
   - Uses `questionary.select` for interactive selection
   - Extracts selected format (removes extension dot)
   - Updates `config.json` with new format
   - Saves updated configuration to file
   - Logs success message

**Key Features**:
- Interactive format selection
- Visual indication of current format
- Automatic config file update
- Validation against valid extensions

**Dependencies**:
- `json` - JSON file operations
- `questionary` - Interactive prompts
- `constants.VALID_AUDIO_EXTENSIONS` - Valid format list
- `utils.logger` - Logging

**Usage**: Called from `menus.tools_menu` to change default audio format.

---

### `compress_music.py`
**Purpose**: Compresses audio files in the music library to a user-selected bitrate.

**How it works**:

1. **`compress_music(config)`**:
   - Scans music directory recursively for audio files
   - Displays bitrate options with descriptions from constants
   - Uses `questionary.select` for bitrate selection
   - For each audio file found:
     - Creates output filename: `"compressed_{original_filename}"`
     - Uses `ffmpeg` to compress file:
       - Command: `ffmpeg -y -i {input} -b:a {bitrate} {output}`
       - `-y` flag overwrites existing files
       - `-b:a` sets audio bitrate
     - Logs success/failure for each file
   - Returns list of compressed file paths
   - Returns `None` if cancelled

**Key Features**:
- Batch compression of entire library
- User-selectable bitrate
- Preserves original files (creates compressed copies)
- Progress logging per file

**Dependencies**:
- `os`, `subprocess` - File operations and ffmpeg execution
- `questionary` - Interactive prompts
- `constants.VALID_AUDIO_EXTENSIONS` - Audio file detection
- `constants.AUDIO_BITRATE_OPTIONS` - Bitrate choices
- `utils.logger` - Logging

**Usage**: Called from `menus.tools_menu` to compress music library.

**Note**: Creates new files with "compressed_" prefix. Original files are preserved.

---

### `dependency_check.py`
**Purpose**: Verifies that all required Python and system dependencies are installed.

**How it works**:

1. **`dependency_check()`**:
   - **Python Dependencies**:
     - Iterates through `PYTHON_DEPENDENCIES` from constants
     - Uses `importlib.import_module()` to test imports
     - Tracks missing dependencies
   - **System Dependencies**:
     - Iterates through `SYSTEM_DEPENDENCIES` from constants
     - Uses `shutil.which()` to check if binary exists in PATH
     - Tracks missing binaries
   - **Requirements.txt Check**:
     - Reads `requirements.txt` if it exists
     - Parses package names (handles version specifiers)
     - Tests imports for packages not in constants list
   - **Output**:
     - Logs missing Python dependencies in red
     - Logs missing system dependencies in red
     - Logs success messages if all dependencies present
   - Returns tuple: `(missing_python, missing_system)`

**Key Features**:
- Comprehensive dependency checking
- Checks both Python packages and system binaries
- Validates requirements.txt
- Clear error reporting

**Dependencies**:
- `subprocess`, `importlib`, `shutil`, `sys`, `os` - System operations
- `utils.logger` - Logging
- `constants.PYTHON_DEPENDENCIES` - Python package list
- `constants.SYSTEM_DEPENDENCIES` - System binary list

**Usage**: Called from `menus.tools_menu` to verify installation.

---

### `library_cleanup.py`
**Purpose**: Removes broken, corrupted, or unreadable audio files from the music library.

**How it works**:

1. **`is_file_corrupted(file_path)`**:
   - Uses `ffmpeg` to test file integrity
   - Command: `ffmpeg -v error -i {file} -f null -`
   - Returns `True` if file is corrupted (non-zero exit code)
   - Returns `True` if ffmpeg command fails
   - Returns `False` if file is valid

2. **`library_cleanup(config)`**:
   - Scans music directory recursively
   - For each file with valid audio extension:
     - Checks file size (removes 0-byte files)
     - Tests file integrity using `is_file_corrupted`
     - Removes corrupted or empty files
     - Logs each removal
   - Counts and reports total removed files
   - Handles errors gracefully

**Key Features**:
- Automatic detection of corrupted files
- Removes empty (0-byte) files
- Uses ffmpeg for integrity checking
- Safe file removal with error handling

**Dependencies**:
- `os`, `subprocess` - File operations and ffmpeg execution
- `constants.VALID_AUDIO_EXTENSIONS` - Audio file detection
- `utils.logger` - Logging

**Usage**: Called from `menus.tools_menu` to clean up broken files.

---

### `library_export_json.py`
**Purpose**: Exports a list of all music files in the library as a JSON file.

**How it works**:

1. **`library_export_json(config)`**:
   - Scans music directory recursively
   - Collects all files with valid audio extensions
   - Stores only filenames (not full paths)
   - Creates `export/` directory if it doesn't exist
   - Generates filename with current date: `harmoni_export_YYYY-MM-DD.json`
   - Creates JSON structure:
     ```json
     {
       "music_files": ["file1.mp3", "file2.mp3", ...]
     }
     ```
   - Saves to export directory
   - Logs success with file count

**Key Features**:
- Date-stamped export files
- Recursive directory scanning
- Only includes valid audio files
- Creates export directory automatically

**Dependencies**:
- `os`, `json` - File operations and JSON
- `datetime` - Date formatting
- `constants.VALID_AUDIO_EXTENSIONS` - Audio file detection
- `utils.logger` - Logging

**Usage**: Called from `menus.tools_menu` to export library inventory.

**Output Location**: `export/harmoni_export_YYYY-MM-DD.json`

---

### `playlist_to_tracklist.py`
**Purpose**: Converts playlist format to flat tracklist format.

**How it works**:

1. **`playlist_to_tracklist(config)`**:
   - Loads `playlists.json` from config
   - Iterates through all playlists
   - For each playlist, iterates through items
   - Extracts track information:
     - `artist` from `track.artistName`
     - `album` from `track.albumName`
     - `track` from `track.trackName`
     - `uri` from `track.trackUri`
   - Creates flat list of all tracks (removes playlist grouping)
   - Creates `export/` directory if needed
   - Saves to `export/playlist_tracklist.json`:
     ```json
     {
       "tracks": [
         {"artist": "...", "album": "...", "track": "...", "uri": "..."},
         ...
       ]
     }
     ```
   - Logs success message

**Key Features**:
- Converts nested playlist structure to flat list
- Preserves all track metadata
- Handles missing fields gracefully
- Creates export directory automatically

**Dependencies**:
- `json`, `os` - File operations
- `utils.logger` - Logging

**Usage**: Called from `menus.tools_menu` to convert playlists to tracklist format.

**Output Location**: `export/playlist_tracklist.json`

---

### `open_log.py`
**Purpose**: Opens the application log file in the system's default text viewer.

**How it works**:

1. **`open_log()`**:
   - Checks if `app.log` exists (from constants)
   - Detects operating system using `platform.system()`
   - Opens log file using platform-specific command:
     - **Windows**: `os.startfile(LOG_FILE)`
     - **macOS**: `subprocess.run(["open", LOG_FILE])`
     - **Linux/Others**: `subprocess.run(["xdg-open", LOG_FILE])`
   - Handles errors gracefully

**Key Features**:
- Cross-platform log file opening
- Uses system default viewer
- Graceful error handling

**Dependencies**:
- `subprocess`, `platform`, `os` - System operations
- `constants.LOG_FILE` - Log file path

**Usage**: Called from `menus.tools_menu` to view application logs.

---

### `__init__.py`
**Purpose**: Makes the tools folder a Python package.

**How it works**: Empty or contains package-level imports/exports.

**Usage**: Allows importing modules from the tools package.

