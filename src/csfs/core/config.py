# SPDX-License-Identifier: GPL-3.0-or-later
"""Load per-provider configuration from a YAML file."""

from __future__ import annotations

from pathlib import Path

import structlog
import yaml

logger = structlog.get_logger()

_DEFAULT_PATHS = (
    Path("csfs.yaml"),
    Path.home() / ".config" / "csfs" / "config.yaml",
)


def load_config(path: Path | None = None) -> dict[str, dict]:
    """Load provider configs from a YAML file.

    Returns a dict mapping provider slugs to their config dicts.
    If no file is found, returns ``{}`` (all connectors work without config).
    """
    if path is not None:
        return _read(path)

    for candidate in _DEFAULT_PATHS:
        if candidate.is_file():
            return _read(candidate)

    return {}


def _read(path: Path) -> dict[str, dict]:
    try:
        data = yaml.safe_load(path.read_text()) or {}
    except Exception as exc:
        logger.warning("config_load_failed", path=str(path), error=str(exc))
        return {}

    providers = data.get("providers", {})
    if not isinstance(providers, dict):
        logger.warning("config_invalid_providers_key", path=str(path))
        return {}

    logger.debug("config_loaded", path=str(path), providers=len(providers))
    return providers
