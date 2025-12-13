# HARMONI Documentation

Welcome to HARMONI documentation! This folder contains detailed documentation for each module and folder in codebase.

## System Requirements

### Required Dependencies
- **Python 3.9+** - Primary runtime environment
- **yt-dlp** - YouTube video/audio extraction tool (installed via pip)
- **ffmpeg** - Audio processing and format conversion (system package)
  - Ubuntu/Debian: `sudo apt install ffmpeg`
  - macOS: `brew install ffmpeg`
  - Windows: Download from https://ffmpeg.org/download.html

### Optional Dependencies
- **JavaScript Runtime** - Recommended for yt-dlp (Node.js, Deno, or QuickJS)
  - Improves YouTube extraction reliability
  - Install via: `npm install -g node` or follow yt-dlp documentation

## Documentation Structure

- **[Root Files](./root-files.md)** - Documentation for main entry points and configuration files
- **[Downloader](./downloader.md)** - Core download functionality and YouTube integration
- **[Menus](./menus.md)** - Interactive CLI menu system
- **[Managers](./managers.md)** - File management, resume, and scheduling utilities
- **[Utils](./utils.md)** - Utility functions for logging, loading, and system checks
- **[Tools](./tools.md)** - Additional utility tools for library management
- **[Data & Export](./data-export.md)** - Data storage and export formats
- **[History](./history.md)** - Historical code and prototypes

## Quick Navigation

### Core Functionality
- **Downloader**: Handles all music downloading logic using yt-dlp
- **Menus**: Provides interactive command-line interface
- **Managers**: Manages files, resumes downloads, and schedules tasks

### Supporting Systems
- **Utils**: Logging, data loading, and system resource checks
- **Tools**: Library cleanup, format conversion, and dependency checks
- **Data**: Stores tracks, playlists, and download history

## Getting Started

1. Start with [Root Files](./root-files.md) to understand the entry point
2. Review [Downloader](./downloader.md) to understand core functionality
3. Explore [Menus](./menus.md) to see how users interact with the system
4. Check [Utils](./utils.md) and [Tools](./tools.md) for additional features

## Finding Information

Each documentation file follows this structure:
- **Folder Overview**: Purpose and role in the system
- **File Documentation**: For each file:
  - **Purpose**: What the file does
  - **How it works**: Key functions and logic flow
  - **Dependencies**: What it imports/uses
  - **Usage**: How it's used in the application

