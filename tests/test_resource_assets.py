from datetime import datetime, timedelta, timezone

from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.strategies import composite

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

@composite
def asset_page_strategy(draw, min_items=3, max_items=10):
    return draw(
        st.lists(
            asset_meta_strategy(),
            min_size=min_items,
            max_size=max_items,
            unique_by=lambda d: d["uid"],  # prevent identical dicts
        )
    )

@settings(max_examples=10)
@given(asset_page_strategy())
def test_rules(assets):
    for asset in assets:
        if asset["submission_count"] > 0:
            assert asset["deployment__last_submission_time"] is not None
    import ipdb;ipdb.set_trace()
