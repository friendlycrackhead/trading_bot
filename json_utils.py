"""
Utility functions for safe JSON file operations.

Provides atomic writes to prevent file corruption on crashes.
"""

import json
import os
from pathlib import Path


def atomic_json_write(file_path, data, indent=2):
    """
    Atomically write JSON data to file.

    Prevents file corruption by writing to a temporary file first,
    then atomically renaming it to the target file.

    If process crashes during write, either:
    - Old file remains intact (temp file discarded)
    - New file is complete (atomic rename succeeded)

    Never leaves a partially-written file.

    Args:
        file_path: Path to JSON file (str or Path object)
        data: Python object to serialize to JSON
        indent: JSON indentation (default: 2)

    Raises:
        IOError: If write fails
    """
    file_path = Path(file_path)

    # Ensure parent directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to temporary file in same directory
    temp_path = file_path.with_suffix('.tmp')

    try:
        # Write to temp file
        with open(temp_path, 'w') as f:
            json.dump(data, f, indent=indent)
            f.flush()  # Ensure data written to disk
            os.fsync(f.fileno())  # Force OS to write to disk

        # Atomic rename (replaces old file)
        # On POSIX systems (macOS, Linux), this is atomic
        temp_path.replace(file_path)

    except Exception as e:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise IOError(f"Failed to write {file_path}: {e}")


def safe_json_read(file_path, default=None):
    """
    Safely read JSON file with fallback.

    Args:
        file_path: Path to JSON file (str or Path object)
        default: Default value if file doesn't exist or is invalid

    Returns:
        Parsed JSON data or default value
    """
    file_path = Path(file_path)

    if not file_path.exists():
        return default if default is not None else {}

    try:
        with open(file_path) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"[WARNING] Failed to read {file_path}: {e}")
        print(f"[WARNING] Using default value")
        return default if default is not None else {}
