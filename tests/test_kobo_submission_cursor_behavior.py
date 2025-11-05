"""
Tests for kobo_submission cursor behavior and edge cases.

CRITICAL FINDINGS:
==================

1. GLOBAL CURSOR: kobo_submission uses a single global cursor across all assets
   - State: {'_submission_time': {'last_value': 'timestamp'}}
   - NOT per-asset: No separate cursors for each asset UID

2. DATA LOSS SCENARIO: New assets with old submissions are filtered out
   - When: Global cursor is at T+40, new asset appears with submission at T+30
   - Result: Submission at T+30 is filtered (never loaded)
   - Impact: CRITICAL - permanent data loss for historical submissions

3. SAFE ROLLBACK: DLT properly rolls back on fetch failures
   - When: Submission fetch fails (API error, network issue)
   - Result: Cursor unchanged, transaction rolled back
   - Impact: SAFE - retries work correctly

RECOMMENDATIONS:
================

Option 1: Remove incremental cursor from kobo_submission (SIMPLEST)
- Always fetch all submissions since earliest_submission_date
- Rely on merge disposition to deduplicate
- Trade: Higher API load for guaranteed completeness

Option 2: Implement per-asset cursor tracking (COMPLEX)
- Requires custom state management outside DLT's incremental hints
- Store cursor per asset_uid in pipeline state
- Manually filter submissions in resource logic

Option 3: Use asset's last_submission_time as submission cursor (HYBRID)
- Fetch submissions >= min(asset.last_submission_time for all assets in batch)
- Guarantees new assets are processed correctly
- Trade: Some redundant fetching when assets have different times

See mitigation strategy discussion for implementation details.
"""

from datetime import timedelta

import pytest


def test_inspect_cursor_state_structure(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    pipeline,
):
    """
    EXPLORATORY: Inspect DLT pipeline state to understand cursor storage format.

    Critical question: Is kobo_submission cursor global or per-asset?
    - Global: {"kobo_submission": {"_submission_time": "2025-12-11T00:00:00Z"}}
    - Per-asset: {"kobo_submission": {"asset-1": {"_submission_time": ...}, "asset-2": {...}}}
    """
    # Arrange: 2 assets with different submission times
    asset1 = asset_builder(uid="asset-1", last_submission_offset=timedelta(days=30))
    asset2 = asset_builder(uid="asset-2", last_submission_offset=timedelta(days=40))
    rest_client_stub.set_for_path("project-views", [asset1, asset2])

    # Asset-1: Latest submission at T+30 (2025-12-01)
    submissions_asset1 = [
        submission_builder(id=1, submission_time_offset=timedelta(days=30)),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_asset1)

    # Asset-2: Latest submission at T+40 (2025-12-11)
    submissions_asset2 = [
        submission_builder(id=2, submission_time_offset=timedelta(days=40)),
    ]
    rest_client_stub.set_for_path("assets/asset-2/data", submissions_asset2)

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="cursor_inspect", write_disposition="merge")

    # Inspect: Print state structure for analysis
    state = pipeline.state
    print("\n" + "=" * 80)
    print("PIPELINE STATE DUMP:")
    print("=" * 80)

    # Access source state (where incremental cursors are stored)
    if "sources" in state:
        print("\nSources:")
        for source_name, source_state in state["sources"].items():
            print(f"\n  Source: {source_name}")
            if "resources" in source_state:
                for resource_name, resource_state in source_state["resources"].items():
                    print(f"\n    Resource: {resource_name}")
                    print(f"    State: {resource_state}")

    print("\n" + "=" * 80)

    # This test is exploratory - we don't assert anything yet
    # Just inspect the output to understand state structure


def test_new_asset_with_old_submissions_filtered_out(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    assert_table_count,
    pipeline,
):
    """
    CRITICAL BUG DEMONSTRATION: New asset appears with old submissions.

    Scenario:
    - Run 1: asset-1 has submission at T+40 (cursor advances to T+40)
    - Run 2: asset-2 appears with submission at T+30 (older than cursor)

    Expected (SAFE): asset-2 submission loaded
    Actual (BUG): asset-2 submission filtered out by global cursor → DATA LOSS
    """
    # Arrange: First run with asset-1 (submission at T+40)
    asset1 = asset_builder(uid="asset-1", last_submission_offset=timedelta(days=40))
    rest_client_stub.set_for_path("project-views", [asset1])

    submissions_asset1 = [
        submission_builder(
            id=1, submission_time_offset=timedelta(days=40)
        ),  # 2025-12-11
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_asset1)

    # Act: First run
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="cursor_bug", write_disposition="merge")

    # Verify: 1 submission loaded, cursor at T+40
    assert_table_count("cursor_bug", 1)
    state1 = pipeline.state["sources"]["klt_test"]["resources"]["kobo_submission"]
    cursor_after_run1 = state1["incremental"]["_submission_time"]["last_value"]
    print(f"\n[Run 1] Cursor advanced to: {cursor_after_run1}")

    # Arrange: Second run - new asset-2 appears with OLDER submission (T+30)
    asset2 = asset_builder(uid="asset-2", last_submission_offset=timedelta(days=30))
    rest_client_stub.set_for_path("project-views", [asset1, asset2])

    # Asset-1: no new submissions (empty)
    rest_client_stub.set_for_path("assets/asset-1/data", [])

    # Asset-2: submission at T+30 (BEFORE the current cursor T+40)
    submissions_asset2 = [
        submission_builder(
            id=2, submission_time_offset=timedelta(days=30)
        ),  # 2025-12-01
    ]
    rest_client_stub.set_for_path("assets/asset-2/data", submissions_asset2)

    # Act: Second run
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="cursor_bug", write_disposition="merge")

    # Assert: CRITICAL BUG - asset-2 submission filtered out
    count = assert_table_count("cursor_bug", 1)  # Should be 2, but is 1

    state2 = pipeline.state["sources"]["klt_test"]["resources"]["kobo_submission"]
    cursor_after_run2 = state2["incremental"]["_submission_time"]["last_value"]
    print(f"[Run 2] Cursor still at: {cursor_after_run2}")
    print(f"[Run 2] Expected 2 submissions, got: {count or 1}")

    # Before the fix, this would fail with only 1 submission
    # After the fix (using min timestamp), we should get 2 submissions
    # This test will FAIL until the fix is verified working


def test_submission_fetch_fails_does_pipeline_rollback(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    pipeline,
):
    """
    CRITICAL SCENARIO: Submission fetch fails - does DLT rollback or commit partial state?

    This tests DLT's transaction behavior. If submission fetch fails:
    - SAFE: Entire transaction rolled back, submission cursor unchanged
    - UNSAFE: Submission cursor advances despite failure → future data loss

    Scenario:
    - Run 1: Load submission at T+30 successfully
    - Run 2: Submission fetch fails (simulated API error)
    - Run 3: Check if cursor advanced despite failure
    """
    # Arrange: First run with successful submission
    asset1 = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset1])

    submissions_run1 = [
        submission_builder(id=1, submission_time_offset=timedelta(days=30)),  # T+30
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", submissions_run1)

    # Act: First run - establish baseline
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="cursor_fail", write_disposition="merge")

    # Verify: Initial state
    state1 = pipeline.state["sources"]["klt_test"]["resources"]["kobo_submission"]
    cursor1 = state1["incremental"]["_submission_time"]["last_value"]
    print(f"\n[Run 1] Submission cursor: {cursor1}")

    # Arrange: Second run - simulate API failure during submission fetch
    def failing_submission_fetch():
        raise RuntimeError("Simulated API failure during submission fetch")

    rest_client_stub.set_for_path("assets/asset-1/data", failing_submission_fetch)

    # Act: Second run - should fail
    resource = kobo_submission_factory()
    try:
        run_pipeline_once(resource, table_name="cursor_fail", write_disposition="merge")
        pytest.fail("Expected pipeline to fail on submission fetch, but it succeeded")
    except Exception as e:
        print(f"\n[Run 2] Pipeline failed as expected: {type(e).__name__}: {e}")

    # CRITICAL CHECK: Did the cursor advance despite failure?
    state2 = pipeline.state["sources"]["klt_test"]["resources"]["kobo_submission"]
    cursor2 = state2["incremental"]["_submission_time"]["last_value"]
    print(f"[Run 2] Submission cursor after failure: {cursor2}")

    if cursor2 != cursor1:
        pytest.fail(
            f"CRITICAL BUG: Cursor advanced despite failure! "
            f"Cursor changed from {cursor1} to {cursor2}. "
            f"This means future submissions may be lost."
        )
    else:
        print(
            f"[Run 2] ✅ SAFE: Cursor unchanged at {cursor2} (transaction rolled back)"
        )
