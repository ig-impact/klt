from datetime import timedelta

import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from klt.resources.kobo_asset import date_modified_hint, last_submission_time_hint


def test_empty_first_page(
    cursor_config,
    kobo_asset_factory,
    run_pipeline_once,
    rest_client_stub,
    query,
):
    """Test that both cursor types handle empty first page correctly."""
    # Arrange
    cursor_field, hint, _ = cursor_config
    rest_client_stub.set([])  # Empty page

    resource = kobo_asset_factory(hint=hint)

    # Act
    table = f"empty_page_{cursor_field}"
    run_pipeline_once(resource, table_name=table, write_disposition="append")

    # Assert - DLT doesn't create table if no data flows through
    result = query(
        f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table}'"
    )
    assert len(result) == 0, "Table should not be created for empty result set"


@pytest.mark.parametrize("parallelized", [True, False], ids=["parallel", "serial"])
def test_all_items_filtered(
    cursor_config,
    parallelized,
    kobo_asset_factory,
    run_pipeline_once,
    rest_client_stub,
    query,
):
    """Test that filtering works consistently across cursor types and parallelization."""
    # Arrange
    cursor_field, hint, _ = cursor_config

    items = [
        {
            "uid": "a",
            "deployment__submission_count": 0,  # filtered
            cursor_field: "2025-11-01T00:00:02.000Z",
        },
        {
            "uid": "b",
            "deployment__submission_count": 0,  # filtered
            cursor_field: "2025-11-01T00:00:02.000Z",
        },
    ]

    rest_client_stub.set(items)
    resource = kobo_asset_factory(hint=hint, parallelized=parallelized)

    # Act
    table = f"all_filtered_{cursor_field}_{parallelized}"
    run_pipeline_once(resource, table_name=table, write_disposition="append")

    # Assert - No table created when all items filtered
    result = query(
        f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table}'"
    )
    assert len(result) == 0, "Table should not be created when all items filtered"


def test_submission_count_null_filtered(
    kobo_asset_all_configs,
    run_pipeline_once,
    assert_table_count,
    rest_client_stub,
):
    """Test that null/missing submission_count is treated as 0 and filtered.

    Note: While the KoboToolbox API consistently returns deployment__submission_count,
    this test ensures defensive handling of null/missing values. The filter logic uses
    `(ka.get("deployment__submission_count") or 0) > 0` to safely handle edge cases.
    """
    # Arrange
    resource, cursor_field, _, parallelized = kobo_asset_all_configs

    # IMPORTANT: Incremental hint runs BEFORE filter, so all items need cursor field
    # for date_modified (on_cursor_value_missing="raise")
    rest_client_stub.set(
        [
            {
                "uid": "a",
                cursor_field: "2025-11-01T00:00:02.000Z",
            },  # missing deployment__submission_count
            {
                "uid": "b",
                "deployment__submission_count": None,
                cursor_field: "2025-11-01T00:00:02.000Z",
            },  # explicit null
            {
                "uid": "c",
                "deployment__submission_count": 1,
                cursor_field: "2025-11-01T00:00:03.000Z",
            },  # keep
        ]
    )

    # Act
    table = f"null_count_{cursor_field}_{parallelized}"
    run_pipeline_once(resource, table_name=table, write_disposition="append")

    # Assert - only 'c' should be loaded
    assert_table_count(table, 1)


# Example 4: Test cursor behavior below initial_value (cursor-agnostic)
def test_cursor_below_initial_value_skipped(
    cursor_config,
    kobo_asset_factory,
    run_pipeline_once,
    get_table_uids,
    rest_client_stub,
    asset_builder,
):
    """Test that items with cursor < initial_value are skipped."""
    # Arrange
    cursor_field, hint, initial_value = cursor_config

    # Build items with cursor values below, equal to, and above initial value
    # initial_value is "2025-11-01T00:00:01.000Z" for both cursors
    items = [
        asset_builder(
            uid="below",
            last_submission_offset=timedelta(milliseconds=-1),
            modified_offset=timedelta(milliseconds=-1),
        ),
        asset_builder(
            uid="equal",
            last_submission_offset=timedelta(0),
            modified_offset=timedelta(0),
        ),
        asset_builder(
            uid="above",
            last_submission_offset=timedelta(milliseconds=1),
            modified_offset=timedelta(milliseconds=1),
        ),
    ]

    rest_client_stub.set(items)
    resource = kobo_asset_factory(hint=hint)

    # Act
    table = f"cursor_initial_{cursor_field}"
    run_pipeline_once(resource, table_name=table, write_disposition="append")

    # Assert - only equal and above should be loaded
    uids = get_table_uids(table)
    assert uids == ["above", "equal"]


# Example 5: Cursor-specific test (date_modified only)
def test_date_modified_missing_raises_with_valid_items(
    kobo_asset_factory,
    run_pipeline_once,
    rest_client_stub,
):
    """Test that date_modified raises even when only one item is missing cursor."""
    # Arrange
    rest_client_stub.set(
        [
            {
                "uid": "a",
                "deployment__submission_count": 1,
                "date_modified": "2025-11-01T00:00:02.000Z",
            },
            {"uid": "b", "deployment__submission_count": 1},  # missing date_modified
        ]
    )

    resource = kobo_asset_factory(hint=date_modified_hint)

    # Act / Assert - should raise PipelineStepFailed wrapping IncrementalCursorPathMissing
    with pytest.raises(PipelineStepFailed) as exc_info:
        run_pipeline_once(
            resource, table_name="dm_partial_missing", write_disposition="append"
        )

    # Verify it's the expected incremental cursor error
    assert "IncrementalCursorPathMissing" in str(exc_info.value)


def test_last_submission_time_missing_included_in_second_run(
    kobo_asset_factory,
    run_twice,
    get_table_uids,
    asset_builder,
):
    """Test that missing cursor values are included even in second run."""
    # Arrange
    resource = kobo_asset_factory(
        hint=last_submission_time_hint, name="lst_missing_second"
    )

    first_pages = [
        [
            asset_builder(
                uid="a",
                last_submission_offset=timedelta(seconds=1),
            )
        ]
    ]

    # Build item and manually remove cursor field to test missing behavior
    item_b = asset_builder(uid="b")
    del item_b["deployment__last_submission_time"]

    second_pages = [[item_b]]

    # Act
    table = "lst_missing_second"
    run_twice(resource, table=table, first_pages=first_pages, second_pages=second_pages)

    # Assert - both should be loaded
    uids = get_table_uids(table)
    assert uids == ["a", "b"]


def test_parallelized_false_respects_submission_filter(
    kobo_asset_factory,
    run_pipeline_once,
    get_table_uids,
    rest_client_stub,
):
    """Test that parallelized=False still applies submission count filter."""
    # Arrange
    rest_client_stub.set(
        [
            {"uid": "a", "deployment__submission_count": 0},  # filtered
            {"uid": "b", "deployment__submission_count": 1},  # keep
        ]
    )

    resource = kobo_asset_factory(parallelized=False)

    # Act
    table = "serial_filter"
    run_pipeline_once(resource, table_name=table, write_disposition="append")

    # Assert
    uids = get_table_uids(table)
    assert uids == ["b"]


@pytest.mark.parametrize("parallelized", [True, False])
def test_second_run_mixed_boundaries(
    cursor_config,
    parallelized,
    kobo_asset_factory,
    run_twice,
    get_table_uids,
    asset_builder,
):
    """
    Test that second run correctly handles items below, equal to, and above cursor.

    Only items >= cursor should be loaded in second run.
    """
    # Arrange
    cursor_field, hint, _ = cursor_config

    # First run establishes cursor at +4 seconds (2025-11-01T00:00:05.000Z)
    first_pages = [
        [
            asset_builder(
                uid="first",
                last_submission_offset=timedelta(seconds=4),
                modified_offset=timedelta(seconds=4),
            )
        ]
    ]

    # Second run has items below, equal, and above the cursor (+4s)
    second_pages = [
        [
            asset_builder(
                uid="below",
                last_submission_offset=timedelta(seconds=4, milliseconds=-1),  # skip
                modified_offset=timedelta(seconds=4, milliseconds=-1),
            ),
            asset_builder(
                uid="equal",
                last_submission_offset=timedelta(seconds=4),  # load
                modified_offset=timedelta(seconds=4),
            ),
            asset_builder(
                uid="above",
                last_submission_offset=timedelta(seconds=4, milliseconds=1),  # load
                modified_offset=timedelta(seconds=4, milliseconds=1),
            ),
        ]
    ]

    resource = kobo_asset_factory(
        hint=hint,
        parallelized=parallelized,
        name=f"mixed_bound_{cursor_field}_{parallelized}",
    )

    # Act
    table = f"mixed_bound_{cursor_field}_{parallelized}"
    run_twice(resource, table=table, first_pages=first_pages, second_pages=second_pages)

    # Assert - should have first, equal, and above (not below)
    uids = get_table_uids(table)
    assert uids == ["above", "equal", "first"]
