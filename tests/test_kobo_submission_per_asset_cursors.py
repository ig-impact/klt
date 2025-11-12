from datetime import timedelta


def test_parallel_assets_with_different_submission_times_no_data_loss(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    assert_table_count,
    query,
    pipeline,
    base_time,
):
    # Arrange: Two assets with different submission time ranges
    asset_a = asset_builder(uid="asset-a", last_submission_offset=timedelta(days=1))
    asset_b = asset_builder(uid="asset-b", last_submission_offset=timedelta(days=1))
    rest_client_stub.set_for_path("project-views", [asset_a, asset_b])

    submissions_asset_a = [
        submission_builder(id=1, submission_time_offset=timedelta(days=1)),
        submission_builder(id=2, submission_time_offset=timedelta(days=4)),
    ]
    rest_client_stub.set_for_path("assets/asset-a/data", submissions_asset_a)

    submissions_asset_b = [
        submission_builder(id=3, submission_time_offset=timedelta(days=2)),
        submission_builder(id=4, submission_time_offset=timedelta(days=3)),
    ]
    rest_client_stub.set_for_path("assets/asset-b/data", submissions_asset_b)

    resource = kobo_submission_factory(submission_time_start="2025-11-01T00:00:01.000Z")
    run_pipeline_once(resource, table_name="kobo_submission", write_disposition="merge")

    assert_table_count("kobo_submission", 4)
