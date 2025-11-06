"""
Tests for normal, realistic incremental loading scenarios.

These tests verify that the submission resource behaves correctly when:
- Cursor advances to max(_submission_time) across all loaded data
- New assets appear and use the global cursor (not per-asset cursor)
- API filtering by _submission_time is applied server-side
- Multiple assets are handled with a single global cursor

IMPORTANT CONTEXT:
- In KoboToolbox, assets cannot have submissions older than deployment time
- deployment__last_submission_time is near real-time (updated when asset list fetched)
- Scenario "new asset with old submissions" is impossible in practice
- These tests validate the ACTUAL behavior we see in production
"""

from datetime import timedelta


def test_single_asset_cursor_advances_to_max_submission_time(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    pipeline,
    run_pipeline_once,
    query,
):
    """
    Verify cursor advances to the maximum _submission_time from loaded data.

    Scenario:
    - Run 1: Load 3 submissions at T+10, T+20, T+30
    - Expected: Cursor should advance to T+30
    - Run 2: Only submissions >= T+30 should be requested from API
    """
    # Arrange: Create asset
    asset = asset_builder(
        uid="asset-1",
        submission_count=3,
        last_submission_offset=timedelta(seconds=30),
    )
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Create submissions at different times
    submissions_run1 = [
        submission_builder(
            id=1, uuid_val="uuid-1", submission_time_offset=timedelta(seconds=10)
        ),
        submission_builder(
            id=2, uuid_val="uuid-2", submission_time_offset=timedelta(seconds=20)
        ),
        submission_builder(
            id=3, uuid_val="uuid-3", submission_time_offset=timedelta(seconds=30)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_run1)

    # Act: Run 1
    resource = kobo_submission_factory(submission_time_start="2025-11-01T00:00:01.000Z")
    run_pipeline_once(resource, table_name="kobo_submission")

    # Assert: All 3 submissions loaded in run 1
    [(count,)] = query("SELECT COUNT(*) FROM kobo_submission")
    assert count == 3, "Run 1 should load all 3 submissions"

    # Arrange: Run 2 - Add new submission at T+40
    # Note: The API stub will filter out T+30 (already loaded) because cursor is now T+30
    submissions_run2 = submissions_run1 + [
        submission_builder(
            id=4, uuid_val="uuid-4", submission_time_offset=timedelta(seconds=40)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_run2)

    # Update asset metadata to reflect new submission
    asset_updated = asset_builder(
        uid="asset-1",
        submission_count=4,
        last_submission_offset=timedelta(seconds=40),
    )
    rest_client_stub.set_for_path("project-views", [asset_updated])

    # Act: Run 2
    run_pipeline_once(resource, table_name="kobo_submission")

    # Assert: Only 1 new submission loaded (T+40)
    # The stub filters out T+10, T+20, T+30 because they're < cursor (T+30)
    [(count,)] = query("SELECT COUNT(*) FROM kobo_submission")
    assert count == 4, "Run 2 should load 1 new submission (total 4)"


def test_multiple_assets_global_cursor_uses_max_across_all(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    pipeline,
    run_pipeline_once,
    query,
):
    """
    Verify that cursor is global across all assets, not per-asset.

    Scenario:
    - Run 1: asset-1 has submissions at T+10, T+20; asset-2 has submissions at T+30, T+40
    - Expected: Global cursor advances to T+40 (max across both assets)
    - Run 2: New submissions in asset-1 must be >= T+40 to be loaded
    """
    # Arrange: Create 2 assets
    asset1 = asset_builder(
        uid="asset-1",
        submission_count=2,
        last_submission_offset=timedelta(seconds=20),
    )
    asset2 = asset_builder(
        uid="asset-2",
        submission_count=2,
        last_submission_offset=timedelta(seconds=40),
    )
    rest_client_stub.set_for_path("project-views", [asset1, asset2])

    # Arrange: asset-1 submissions at T+10, T+20
    submissions_asset1_run1 = [
        submission_builder(
            id=1, uuid_val="uuid-1-1", submission_time_offset=timedelta(seconds=10)
        ),
        submission_builder(
            id=2, uuid_val="uuid-1-2", submission_time_offset=timedelta(seconds=20)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_asset1_run1)

    # Arrange: asset-2 submissions at T+30, T+40
    submissions_asset2_run1 = [
        submission_builder(
            id=1, uuid_val="uuid-2-1", submission_time_offset=timedelta(seconds=30)
        ),
        submission_builder(
            id=2, uuid_val="uuid-2-2", submission_time_offset=timedelta(seconds=40)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-2/data", submissions_asset2_run1)

    # Act: Run 1
    resource = kobo_submission_factory(submission_time_start="2025-11-01T00:00:01.000Z")
    run_pipeline_once(resource, table_name="kobo_submission")

    # Assert: All 4 submissions loaded
    [(count,)] = query("SELECT COUNT(*) FROM kobo_submission")
    assert count == 4, "Run 1 should load all 4 submissions from both assets"

    # Arrange: Run 2 - Add new submission to asset-1 at T+35 (between T+30 and T+40)
    # This should NOT be loaded because global cursor is T+40
    submissions_asset1_run2 = submissions_asset1_run1 + [
        submission_builder(
            id=3, uuid_val="uuid-1-3", submission_time_offset=timedelta(seconds=35)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_asset1_run2)

    # Act: Run 2
    run_pipeline_once(resource, table_name="kobo_submission")

    # Assert: Still 4 submissions (T+35 is filtered by API because < cursor T+40)
    [(count,)] = query("SELECT COUNT(*) FROM kobo_submission")
    assert count == 4, "Run 2 should not load T+35 submission (< cursor T+40)"


def test_new_asset_appears_uses_global_cursor(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    pipeline,
    run_pipeline_once,
    query,
):
    """
    Verify that when a new asset appears, it uses the global cursor.

    IMPORTANT: In real KoboToolbox workflow, this is the ONLY realistic scenario.
    New assets cannot have submissions older than their deployment time, and
    deployment__last_submission_time is near real-time.

    Scenario:
    - Run 1: asset-1 has submissions up to T+40 → cursor = T+40
    - Run 2: asset-2 appears with submissions at T+50, T+60
    - Expected: asset-2 submissions are loaded starting from T+40 (global cursor)
    """
    # Arrange: Run 1 - Only asset-1
    asset1 = asset_builder(
        uid="asset-1",
        submission_count=2,
        last_submission_offset=timedelta(seconds=40),
    )
    rest_client_stub.set_for_path("project-views", [asset1])

    submissions_asset1 = [
        submission_builder(
            id=1, uuid_val="uuid-1-1", submission_time_offset=timedelta(seconds=30)
        ),
        submission_builder(
            id=2, uuid_val="uuid-1-2", submission_time_offset=timedelta(seconds=40)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_asset1)

    # Act: Run 1
    resource = kobo_submission_factory(submission_time_start="2025-11-01T00:00:01.000Z")
    run_pipeline_once(resource, table_name="kobo_submission")

    # Assert: 2 submissions from asset-1
    [(count,)] = query("SELECT COUNT(*) FROM kobo_submission")
    assert count == 2, "Run 1 should load 2 submissions from asset-1"

    # Arrange: Run 2 - asset-2 appears with NEW submissions at T+50, T+60
    asset2 = asset_builder(
        uid="asset-2",
        submission_count=2,
        last_submission_offset=timedelta(seconds=60),
    )
    rest_client_stub.set_for_path("project-views", [asset1, asset2])

    submissions_asset2 = [
        submission_builder(
            id=1, uuid_val="uuid-2-1", submission_time_offset=timedelta(seconds=50)
        ),
        submission_builder(
            id=2, uuid_val="uuid-2-2", submission_time_offset=timedelta(seconds=60)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-2/data", submissions_asset2)

    # Act: Run 2
    run_pipeline_once(resource, table_name="kobo_submission")

    # Assert: 2 new submissions from asset-2 (total 4)
    [(count,)] = query("SELECT COUNT(*) FROM kobo_submission")
    assert count == 4, "Run 2 should load 2 new submissions from asset-2 (total 4)"


def test_cursor_from_actual_data_not_asset_metadata(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    pipeline,
    run_pipeline_once,
    query,
):
    """
    Verify cursor advances based on actual loaded data, not asset metadata.

    This tests that deployment__last_submission_time metadata is only used for
    min() logic (which we're removing), and cursor comes from actual _submission_time.

    Scenario:
    - asset.deployment__last_submission_time = T+50
    - But actual loaded submissions max = T+40
    - Expected: Cursor should be T+40 (from data), not T+50 (from metadata)
    """
    # Arrange: Asset metadata says T+50, but actual submissions only go to T+40
    asset = asset_builder(
        uid="asset-1",
        submission_count=2,
        last_submission_offset=timedelta(seconds=50),  # Metadata says T+50
    )
    rest_client_stub.set_for_path("project-views", [asset])

    # Actual submissions only go to T+40 (< metadata value)
    submissions_run1 = [
        submission_builder(
            id=1, uuid_val="uuid-1", submission_time_offset=timedelta(seconds=30)
        ),
        submission_builder(
            id=2, uuid_val="uuid-2", submission_time_offset=timedelta(seconds=40)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_run1)

    # Act: Run 1
    resource = kobo_submission_factory(submission_time_start="2025-11-01T00:00:01.000Z")
    run_pipeline_once(resource, table_name="kobo_submission")

    # Arrange: Run 2 - Add submission at T+45 (between actual max T+40 and metadata T+50)
    submissions_run2 = submissions_run1 + [
        submission_builder(
            id=3, uuid_val="uuid-3", submission_time_offset=timedelta(seconds=45)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_run2)

    # Act: Run 2
    run_pipeline_once(resource, table_name="kobo_submission")

    # Assert: T+45 submission should be loaded (cursor was T+40, not T+50)
    [(count,)] = query("SELECT COUNT(*) FROM kobo_submission")
    assert count == 3, "Run 2 should load T+45 submission (cursor was T+40 from data)"


def test_submissions_before_cursor_filtered_by_api(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    pipeline,
    run_pipeline_once,
    query,
):
    """
    Verify that API filtering by _submission_time works correctly in stub.

    This is a test of our test infrastructure - ensuring the RESTClientStub
    correctly simulates the KoboToolbox API's server-side filtering behavior.

    Scenario:
    - Stub returns 5 submissions at T+10, T+20, T+30, T+40, T+50
    - Request with cursor = T+30
    - Expected: Only T+30, T+40, T+50 should be yielded (>= cursor)
    """
    # Arrange: Create asset
    asset = asset_builder(
        uid="asset-1",
        submission_count=5,
        last_submission_offset=timedelta(seconds=50),
    )
    rest_client_stub.set_for_path("project-views", [asset])

    # All 5 submissions available in stub
    all_submissions = [
        submission_builder(
            id=1, uuid_val="uuid-1", submission_time_offset=timedelta(seconds=10)
        ),
        submission_builder(
            id=2, uuid_val="uuid-2", submission_time_offset=timedelta(seconds=20)
        ),
        submission_builder(
            id=3, uuid_val="uuid-3", submission_time_offset=timedelta(seconds=30)
        ),
        submission_builder(
            id=4, uuid_val="uuid-4", submission_time_offset=timedelta(seconds=40)
        ),
        submission_builder(
            id=5, uuid_val="uuid-5", submission_time_offset=timedelta(seconds=50)
        ),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", all_submissions)

    # Act: Load with cursor = T+30 (should get T+30, T+40, T+50)
    resource = kobo_submission_factory(
        submission_time_start="2025-11-01T00:00:31.000Z"
    )  # T+30
    run_pipeline_once(resource, table_name="kobo_submission")

    # Assert: Only 3 submissions loaded (>= T+30)
    results = query("SELECT _id FROM kobo_submission ORDER BY _id")
    loaded_ids = [r[0] for r in results]
    assert loaded_ids == [3, 4, 5], (
        "API should filter to only submissions >= cursor (T+30)"
    )
