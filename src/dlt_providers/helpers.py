import os
from typing import Any

import dlt


def set_dlt_config(config: dict[str, Any], *, prefix: str = "") -> None:
    """
    Set DLT secrets recursively from a configuration dictionary.
    Supports environment variable resolution for values prefixed with 'env:'.

    Args:
        config: Dictionary with configuration values
        prefix: Dot-separated path prefix for nested keys (internal use)
    """
    for key, value in config.items():
        full_key = f"{prefix}.{key}" if prefix else key

        if isinstance(value, dict):
            set_dlt_config(value, prefix=full_key)
            continue

        if isinstance(value, str) and value.startswith("env:"):
            value = os.getenv(value[4:])  # Strip 'env:' prefix

        dlt.secrets[full_key] = value
