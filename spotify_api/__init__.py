"""Spotify Web API integration (OAuth PKCE).

This package is intentionally lightweight and stdlib-only.

Future integration points:
- menus/ (interactive auth + playlist browsing)
- utils/loaders.py (import tracks/playlists directly from Spotify)
"""

from .auth import SpotifyPKCEAuth
from .client import SpotifyClient
from .data_loader import SpotifyDataLoader
from .token_manager import TokenManager

__all__ = [
    "SpotifyPKCEAuth",
    "SpotifyClient",
    "SpotifyDataLoader",
    "TokenManager",
]
