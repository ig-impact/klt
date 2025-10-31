import shutil
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import dlt
from hypothesis import strategies as st
from hypothesis.strategies import composite


@contextmanager
def pipeline_ctx(
    base_dir: Path,
    *,
    name: str = "klt",
    dataset: str = "klt_dataset",
    destination: str = "duckdb",
):
    """Create an isolated dlt pipeline in a unique subdir under base_dir.
    Ensures pipeline.drop() and directory cleanup on exit.
    """
    run_dir = base_dir / f"run_{uuid4().hex}"
    run_dir.mkdir(parents=True, exist_ok=False)

    p = dlt.pipeline(
        pipeline_name=name,
        destination=destination,
        dataset_name=dataset,
        pipelines_dir=run_dir,
    )
    try:
        yield p
    finally:
        # Best-effort cleanup: drop pipeline, then remove the directory
        try:
            p.drop()
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)


MIN_DT = datetime(2000, 1, 1)
MAX_DT = datetime(2100, 12, 31)
HEADROOM = timedelta(days=60)


@composite
def asset_meta_strategy(draw):
    date_created = draw(
        st.datetimes(
            min_value=MIN_DT,
            max_value=MAX_DT - HEADROOM,
            timezones=st.just(timezone.utc),
        )
    )

    # ensure modified is strictly after created, but within headroom
    mod_days = draw(st.integers(min_value=1, max_value=30))
    mod_secs = draw(st.integers(min_value=0, max_value=86_399))
    date_modified = date_created + timedelta(days=mod_days, seconds=mod_secs)

    # TODO: deploy can happen after modified
    # deploy somewhere between created and modified (inclusive)
    span_secs = int((date_modified - date_created).total_seconds())
    deploy_offset = draw(st.integers(min_value=0, max_value=span_secs))
    date_deployed = date_created + timedelta(seconds=deploy_offset)

    submission_count = draw(st.integers(min_value=0, max_value=10_000))
    if submission_count == 0:
        last_sub_time = None
    else:
        sub_offset = draw(st.integers(min_value=0, max_value=span_secs))
        last_sub_time = date_created + timedelta(seconds=sub_offset)

    uid = draw(st.uuids().map(str))

    return {
        "uid": uid,
        "date_created": date_created,
        "date_modified": date_modified,
        "date_deployed": date_deployed,
        "submission_count": submission_count,
        "deployment__last_submission_time": last_sub_time,
    }


def asset_page_strategy(min_items=3, max_items=50):
    return st.lists(
        asset_meta_strategy(),
        min_size=min_items,
        max_size=max_items,
        unique_by=lambda d: d["uid"],
    )
