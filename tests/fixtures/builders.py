from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional
import uuid

import pytest


@pytest.fixture(scope="session")
def base_time() -> datetime:
    """
    A fixed, timezone-aware baseline. Derive all timestamps as base_time ± delta
    to keep tests deterministic and readable.
    """
    return datetime(2025, 1, 16, 9, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="function")
def asset_builder(base_time) -> Callable[..., Dict]:
    """
    Tiny factory for a single 'asset' dict with sensible defaults.
    Override any field via kwargs; most common overrides are shown as parameters.
    """

    def build(
        *,
        uid: Optional[str] = None,
        submission_count: int = 1,
        last_submission_offset: timedelta = timedelta(0),
        modified_offset: timedelta = timedelta(0),
        extra: Optional[Dict] = None,
    ) -> Dict:
        uid = uid or str(uuid.uuid4())
        row = {
            "uid": uid,
            "deployment__submission_count": submission_count,
            "deployment__last_submission_time": (
                base_time + last_submission_offset
            ).isoformat(),
            "date_modified": (base_time + modified_offset).isoformat(),
        }
        if extra:
            row.update(extra)
        return row

    return build


@pytest.fixture(scope="function")
def asset_page_builder(asset_builder) -> Callable[..., List[Dict]]:
    """
    Build a list[dict] 'page' in one line.
    Accepts either:
      - a list of per-row kwargs for asset_builder, or
      - an int N to produce N identical default rows (rare).
    """

    def build_page(
        rows: Optional[List[Dict]] = None, *, n: Optional[int] = None
    ) -> List[Dict]:
        if rows is not None and n is not None:
            raise ValueError("Provide either 'rows' or 'n', not both.")
        if rows is not None:
            return [asset_builder(**r) for r in rows]
        if n is not None:
            return [asset_builder() for _ in range(n)]
        # default: single-row page
        return [asset_builder()]

    return build_page
