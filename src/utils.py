"""Utility functions for file operations and configuration."""

import os
import re
import logging
import yaml
from dotenv import load_dotenv
from typing import Optional

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yaml") -> dict:
    """Load YAML configuration file.

    Args:
        config_path: Path to the YAML config file.

    Returns:
        Dictionary with configuration parameters.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_env_credentials() -> tuple[str, str]:
    """Load login credentials from .env file.

    Returns:
        Tuple of (login, password).

    Raises:
        ValueError: If credentials are not found in .env.
    """
    load_dotenv()
    login = os.getenv("CHEMORBIS_LOGIN")
    password = os.getenv("CHEMORBIS_PASSWORD")

    if not login or not password:
        raise ValueError(
            "CHEMORBIS_LOGIN and CHEMORBIS_PASSWORD must be set in .env file. "
            "See .env.example for reference."
        )

    return login, password


def get_env_path(env_var: str, default: str = "") -> str:
    """Get a filesystem path from environment variable.

    Args:
        env_var: Name of the environment variable.
        default: Default value if not set.

    Returns:
        The path string.
    """
    load_dotenv()
    return os.getenv(env_var, default)


def get_latest_file(directory: str) -> Optional[str]:
    """Find the most recently modified file in a directory.

    Args:
        directory: Path to the directory to search.

    Returns:
        Full path to the newest file, or None if directory is empty.
    """
    if not os.path.exists(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return None

    files = [
        f for f in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, f))
    ]

    if not files:
        return None

    files.sort(
        key=lambda x: os.path.getmtime(os.path.join(directory, x)),
        reverse=True
    )
    return os.path.join(directory, files[0])


def remove_extra_spaces(text: str) -> str:
    """Collapse multiple whitespace characters into single spaces.

    Args:
        text: Input string.

    Returns:
        Cleaned string with normalized whitespace.
    """
    return re.sub(r"\s+", " ", text).strip()


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging for the application.

    Args:
        level: Logging level (default: INFO).
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("scraper.log", encoding="utf-8"),
        ],
    )