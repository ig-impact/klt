from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional
import uuid

import pytest


@pytest.fixture(scope="session")
def base_time() -> datetime:
    """
    A fixed, timezone-aware baseline matching the initial_value in incremental hints.
    Derive all timestamps as base_time ± delta to keep tests deterministic and readable.

    This corresponds to "2025-11-01T00:00:01.000Z" (the initial_value in kobo_asset hints).
    """
    return datetime(2025, 11, 1, 0, 0, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="function")
def asset_builder(base_time) -> Callable[..., Dict]:
    """
    Tiny factory for a single 'asset' dict with sensible defaults.
    Override any field via kwargs; most common overrides are shown as parameters.
    """

    def _format_timestamp(dt: datetime) -> str:
        """Format datetime to ISO8601 with millisecond precision and Z suffix."""
        # Format with microseconds, then truncate to milliseconds (3 digits)
        iso_str = dt.isoformat(timespec="milliseconds")
        # Replace timezone offset with Z
        return iso_str.replace("+00:00", "Z")

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
            "deployment__last_submission_time": _format_timestamp(
                base_time + last_submission_offset
            ),
            "date_modified": _format_timestamp(base_time + modified_offset),
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
