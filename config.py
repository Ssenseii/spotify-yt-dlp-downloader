import json
import os
from typing import Any, Dict, Optional

CONFIG_PATH = "config.json"

# Default configuration values
DEFAULT_CONFIG = {
    "tracks_file": "data/tracks.json",
    "playlists_file": "data/playlists.json",

    # CSV-first inputs (Exportify format)
    "primary_input_source": "csv",  # "csv" (preferred) or "json" (legacy)
    "primary_csv_file": "../_WORKING/Electro.csv",

    "output_dir": "music",
    "audio_format": "mp3",
    "sleep_between": 5,
    "average_download_time": 20,
    "retry_attempts": 3,
    "retry_delay": 5,
    "auto_cleanup": False,
    "auto_backup": True,
    "max_backups": 10,
    "profile": "light",
    "exportify_watch_folder": "data/exportify",

    # Sync behavior
    "sync_write_tracks_json": True,
    "auto_sync_enabled": False,
    "auto_sync_interval": 3600,
}

# Profile definitions
CONFIG_PROFILES = {
    "light": {
        "retry_attempts": 1,
        "retry_delay": 3,
        "auto_cleanup": False,
        "auto_backup": True,
        "max_backups": 5,
        "sleep_between": 3,
    },
    "advanced": {
        "retry_attempts": 5,
        "retry_delay": 10,
        "auto_cleanup": False,
        "auto_backup": True,
        "max_backups": 20,
        "sleep_between": 5,
    },
    "minimal": {
        "retry_attempts": 0,
        "retry_delay": 0,
        "auto_cleanup": False,
        "auto_backup": False,
        "max_backups": 0,
        "sleep_between": 2,
    },
}

# Validation rules for config fields
CONFIG_SCHEMA = {
    "tracks_file": {"type": str, "required": True},
    "playlists_file": {"type": str, "required": True},

    "primary_input_source": {"type": str, "required": False, "choices": ["csv", "json"]},
    "primary_csv_file": {"type": str, "required": False},

    "output_dir": {"type": str, "required": True},
    "audio_format": {
        "type": str,
        "required": True,
        "choices": ["mp3", "wav", "flac", "aac", "ogg", "m4a"],
    },
    "sleep_between": {"type": (int, float), "required": True, "min": 0, "max": 60},
    "average_download_time": {"type": (int, float), "required": False, "min": 1, "max": 300},
    "retry_attempts": {"type": int, "required": False, "min": 0, "max": 10},
    "retry_delay": {"type": (int, float), "required": False, "min": 0, "max": 60},
    "auto_cleanup": {"type": bool, "required": False},
    "auto_backup": {"type": bool, "required": False},
    "max_backups": {"type": int, "required": False, "min": 0, "max": 100},
    "profile": {"type": str, "required": False, "choices": ["light", "advanced", "minimal"]},
    "exportify_watch_folder": {"type": str, "required": False},

    "sync_write_tracks_json": {"type": bool, "required": False},
    "auto_sync_enabled": {"type": bool, "required": False},
    "auto_sync_interval": {"type": int, "required": False, "min": 60, "max": 86400},
}


def load_config() -> Dict[str, Any]:
    """Load configuration from file, applying defaults for missing fields."""
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Config file {CONFIG_PATH} not found.")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Apply defaults for missing fields
    for key, value in DEFAULT_CONFIG.items():
        if key not in config:
            config[key] = value

    return config


def save_config(config: Dict[str, Any]) -> bool:
    """Save configuration to file."""
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        raise IOError(f"Failed to save config: {e}")


def validate_config(config: Dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Validate configuration against schema.
    Returns (is_valid, list_of_errors).
    """
    errors = []

    for key, rules in CONFIG_SCHEMA.items():
        # Check required fields
        if rules.get("required", False) and key not in config:
            errors.append(f"Missing required field: {key}")
            continue

        if key not in config:
            continue

        value = config[key]

        # Type check
        expected_type = rules.get("type")
        if expected_type and not isinstance(value, expected_type):
            type_names = expected_type.__name__ if not isinstance(expected_type, tuple) else "/".join(t.__name__ for t in expected_type)
            errors.append(f"Field '{key}' must be {type_names}, got {type(value).__name__}")
            continue

        # Choices check
        if "choices" in rules and value not in rules["choices"]:
            errors.append(f"Field '{key}' must be one of {rules['choices']}, got '{value}'")

        # Range check for numeric values
        if isinstance(value, (int, float)):
            if "min" in rules and value < rules["min"]:
                errors.append(f"Field '{key}' must be >= {rules['min']}, got {value}")
            if "max" in rules and value > rules["max"]:
                errors.append(f"Field '{key}' must be <= {rules['max']}, got {value}")

    return len(errors) == 0, errors


def update_config(key: str, value: Any) -> tuple[bool, str]:
    """
    Update a single config field with validation.
    Returns (success, message).
    """
    config = load_config()

    # Check if key is valid
    if key not in CONFIG_SCHEMA:
        return False, f"Unknown config key: {key}"

    # Create temporary config with new value
    test_config = config.copy()
    test_config[key] = value

    # Validate the change
    is_valid, errors = validate_config(test_config)
    if not is_valid:
        return False, f"Validation failed: {', '.join(errors)}"

    # Save the updated config
    config[key] = value
    save_config(config)

    return True, f"Updated '{key}' to '{value}'"


def get_config_profile(config: Dict[str, Any]) -> str:
    """Get the current profile name from config."""
    return config.get("profile", "light")


def apply_config_profile(profile_name: str) -> tuple[bool, str]:
    """
    Apply a configuration profile, updating relevant settings.
    Returns (success, message).
    """
    if profile_name not in CONFIG_PROFILES:
        return False, f"Unknown profile: {profile_name}. Available: {list(CONFIG_PROFILES.keys())}"

    config = load_config()
    profile_settings = CONFIG_PROFILES[profile_name]

    # Apply profile settings
    for key, value in profile_settings.items():
        config[key] = value

    config["profile"] = profile_name

    # Validate and save
    is_valid, errors = validate_config(config)
    if not is_valid:
        return False, f"Profile validation failed: {', '.join(errors)}"

    save_config(config)
    return True, f"Applied profile '{profile_name}' successfully"


def get_profile_info(profile_name: str) -> Optional[Dict[str, Any]]:
    """Get settings for a specific profile."""
    return CONFIG_PROFILES.get(profile_name)


def list_profiles() -> Dict[str, Dict[str, Any]]:
    """Return all available profiles and their settings."""
    return CONFIG_PROFILES.copy()


def reset_to_defaults() -> tuple[bool, str]:
    """Reset configuration to default values."""
    try:
        save_config(DEFAULT_CONFIG.copy())
        return True, "Configuration reset to defaults"
    except Exception as e:
        return False, f"Failed to reset config: {e}"


def get_config_value(key: str, default: Any = None) -> Any:
    """Get a single config value with optional default."""
    try:
        config = load_config()
        return config.get(key, default)
    except Exception:
        return default
