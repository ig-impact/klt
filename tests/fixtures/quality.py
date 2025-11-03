from __future__ import annotations

import logging
from typing import Dict

import pytest


@pytest.fixture(scope="session", autouse=True)
def mute_logs():
    """
    Keep test output clean by muting noisy libraries at session start.
    Restores prior levels on teardown.
    """
    targets = [
        "",  # root
        "dlt",
        "dlt.pipeline",
        "dlt.extract",
        "dlt.load",
        "dlt.sources",
        "urllib3",
        "httpx",
    ]

    # Capture current levels to restore later
    prior_levels: Dict[str, int] = {}
    for name in targets:
        logger = logging.getLogger(name)
        prior_levels[name] = logger.level
        logger.setLevel(logging.WARNING)

    # Also ensure basicConfig doesn't re-emit DEBUG from deps
    logging.basicConfig(level=logging.WARNING, force=False)

    yield

    # Restore previous levels
    for name, level in prior_levels.items():
        logging.getLogger(name).setLevel(level)
