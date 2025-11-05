from datetime import timedelta

import pytest


def test_paginates_across_pages(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    assert_table_count,
):
    """Verify pagination works for submission data from a single asset."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Create submission pages for this asset
    page1 = [
        submission_builder(id=1, submission_time_offset=timedelta(days=30)),
        submission_builder(id=2, submission_time_offset=timedelta(days=31)),
    ]
    page2 = [
        submission_builder(id=3, submission_time_offset=timedelta(days=32)),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", page1, page2)

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="submissions")

    # Assert
    assert_table_count("submissions", 3)


def test_composite_primary_key_enforced(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    assert_table_count,
):
    """Load submissions with duplicate _id + _uuid combinations, verify deduplication."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Create submissions with duplicate composite keys
    page1 = [
        submission_builder(
            id=1, uuid_val="uuid-a", submission_time_offset=timedelta(days=30)
        ),
        submission_builder(
            id=2, uuid_val="uuid-b", submission_time_offset=timedelta(days=31)
        ),
    ]
    page2 = [
        submission_builder(
            id=1, uuid_val="uuid-a", submission_time_offset=timedelta(days=32)
        ),  # Duplicate
        submission_builder(
            id=3, uuid_val="uuid-c", submission_time_offset=timedelta(days=33)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", page1, page2)

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(
        resource, table_name="submissions_dedup", write_disposition="merge"
    )

    # Assert: Only 3 unique rows (deduplication by composite key)
    assert_table_count("submissions_dedup", 3)


def test_incremental_cursor_advances(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    assert_table_count,
):
    """First run: older submissions filtered. Second run: only newer submissions."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: First run - submissions after initial_value (2025-01-01)
    first_submissions = [
        submission_builder(
            id=1, submission_time_offset=timedelta(days=30)
        ),  # 2025-12-01
        submission_builder(
            id=2, submission_time_offset=timedelta(days=31)
        ),  # 2025-12-02
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", first_submissions)

    # Act: First run
    resource = kobo_submission_factory()
    run_pipeline_once(
        resource, table_name="submissions_incremental", write_disposition="append"
    )

    # Arrange: Second run - newer submissions (update path routing)
    second_submissions = [
        submission_builder(
            id=3, submission_time_offset=timedelta(days=40)
        ),  # 2025-12-11
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", second_submissions)

    # Act: Second run
    resource = kobo_submission_factory()
    run_pipeline_once(
        resource, table_name="submissions_incremental", write_disposition="append"
    )

    # Assert: All 3 submissions loaded
    assert_table_count("submissions_incremental", 3)


def test_cursor_missing_raises(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
):
    """Load submission without _submission_time field, verify error is raised."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Create submission missing _submission_time
    submission_missing_cursor = {
        "_id": 1,
        "_uuid": "uuid-a",
        "_submitted_by": "test_user",
        "question1": "answer1",
    }
    rest_client_stub.set_for_path("assets/asset-1/data", [submission_missing_cursor])

    # Act & Assert
    resource = kobo_submission_factory()
    with pytest.raises(Exception):  # dlt raises when cursor field is missing
        run_pipeline_once(resource, table_name="submissions_missing_cursor")


def test_empty_submissions_from_asset(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    run_pipeline_once,
    query,
):
    """Asset exists but has zero submissions, verify no error and table empty."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: No submissions for this asset
    rest_client_stub.set_for_path("assets/asset-1/data", [])

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="submissions_empty")

    # Assert: Table should not be created (dlt behavior with empty data)
    result = query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'submissions_empty'"
    )
    assert len(result) == 0, "Table should not be created for empty result set"


def test_earliest_submission_date_parameter(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    assert_table_count,
):
    """Override earliest_submission_date to custom value, verify initial cursor starts from that date."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Submissions with different dates
    # base_time is 2025-11-01, so timedelta(days=120) = ~2026-03-01
    submissions = [
        submission_builder(
            id=1, submission_time_offset=timedelta(days=100)
        ),  # 2026-02-09
        submission_builder(
            id=2, submission_time_offset=timedelta(days=120)
        ),  # 2026-03-01
        submission_builder(
            id=3, submission_time_offset=timedelta(days=140)
        ),  # 2026-03-21
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions)

    # Act: Use custom earliest_submission_date of 2026-03-01
    resource = kobo_submission_factory(earliest_submission_date="2026-03-01T00:00:00Z")
    run_pipeline_once(resource, table_name="submissions_custom_date")

    # Assert: Only 2 submissions should be loaded (>= 2026-03-01)
    assert_table_count("submissions_custom_date", 2)


def test_multiple_assets_yield_combined_submissions(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    assert_table_count,
):
    """2 assets, each with submissions, verify all submissions loaded to same table."""
    # Arrange: Create 2 assets
    asset1 = asset_builder(uid="asset-1")
    asset2 = asset_builder(uid="asset-2")
    rest_client_stub.set_for_path("project-views", [asset1, asset2])

    # Arrange: Submissions for asset-1
    submissions_asset1 = [
        submission_builder(id=1, submission_time_offset=timedelta(days=30)),
        submission_builder(id=2, submission_time_offset=timedelta(days=31)),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_asset1)

    # Arrange: Submissions for asset-2
    submissions_asset2 = [
        submission_builder(id=3, submission_time_offset=timedelta(days=32)),
        submission_builder(id=4, submission_time_offset=timedelta(days=33)),
    ]
    rest_client_stub.set_for_path("assets/asset-2/data", submissions_asset2)

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="submissions_multi_asset")

    # Assert: All 4 submissions from both assets loaded
    assert_table_count("submissions_multi_asset", 4)
