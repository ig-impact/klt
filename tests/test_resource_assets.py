import pytest

from klt.resources.kobo_asset import (
    date_modified_hint,
    last_submission_time_hint,
    make_resource_kobo_asset,
)


def test_filters_submission_count(
    run_pipeline_once, assert_table_count, rest_client_stub
):
    # Arrange
    rest_client_stub.set(
        [
            {"uid": "a", "deployment__submission_count": 0},  # filtered
            {"uid": "b", "deployment__submission_count": 2},  # keep
            {"uid": "c", "deployment__submission_count": 1},  # keep
        ]
    )
    resource = make_resource_kobo_asset(rest_client_stub, kobo_project_view_uid="TEST")

    # Act
    table = "asset_filter_min"
    run_pipeline_once(resource, table_name=table, write_disposition="append")

    # Assert
    assert_table_count(table, 2)


def test_paginates_across_pages(
    run_pipeline_once, assert_table_count, rest_client_stub
):
    # Arrange: two pages; one filtered row on the second
    rest_client_stub.set(
        [
            {"uid": "a", "deployment__submission_count": 1},
            {"uid": "b", "deployment__submission_count": 1},
        ],
        [
            {"uid": "c", "deployment__submission_count": 0},  # filtered
            {"uid": "d", "deployment__submission_count": 3},
        ],
    )
    resource = make_resource_kobo_asset(rest_client_stub, kobo_project_view_uid="TEST")

    # Act
    table = "asset_paginate_min"
    run_pipeline_once(resource, table_name=table, write_disposition="append")

    # Assert
    assert_table_count(table, 3)


def test_date_modified_missing_raises(run_pipeline_once, rest_client_stub):
    # Arrange: passes submission filter but lacks date_modified
    rest_client_stub.set([{"uid": "x", "deployment__submission_count": 1}])
    base = make_resource_kobo_asset(rest_client_stub, kobo_project_view_uid="TEST")
    resource = base.apply_hints(incremental=date_modified_hint)

    # Act / Assert: on_cursor_value_missing="raise" should bubble up
    with pytest.raises(Exception):
        run_pipeline_once(
            resource, table_name="dm_missing_raises", write_disposition="append"
        )


def test_last_submission_time_missing_is_included(
    run_pipeline_once, assert_table_count, rest_client_stub
):
    # Arrange: passes submission filter but lacks deployment__last_submission_time
    rest_client_stub.set([{"uid": "y", "deployment__submission_count": 1}])
    base = make_resource_kobo_asset(
        rest_client_stub, kobo_project_view_uid="TEST", name="kobo_asset_for_data"
    )
    resource = base.apply_hints(incremental=last_submission_time_hint)

    # Act: include on missing => should not raise and should land
    table = "lst_missing_included_min"
    run_pipeline_once(resource, table_name=table, write_disposition="append")

    # Assert
    assert_table_count(table, 1)
