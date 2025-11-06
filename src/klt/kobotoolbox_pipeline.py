import dlt
from dlt.sources.helpers.rest_client.client import RESTClient

from klt.resources import (
    make_last_submission_time_hint,
    make_resource_kobo_asset,
    make_resource_kobo_submission,
)
from klt.rest_client import make_rest_client


@dlt.source
def kobo_source(
    kobo_token: str = dlt.secrets.value,
    kobo_server: str = dlt.secrets.value,
    kobo_project_view: str = dlt.secrets.value,
    submission_time_start: str = "2025-01-01T00:00:00Z",
    asset_last_submission_start: str = "2025-11-01T00:00:01.000Z",
    asset_modified_start: str = "2025-11-01T00:00:01.000Z",
):
    kobo_client: RESTClient = make_rest_client(kobo_token, kobo_server)

    # Create incremental hint for asset last submission time
    last_submission_time_hint = make_last_submission_time_hint(
        asset_last_submission_start
    )

    kobo_asset = make_resource_kobo_asset(
        kobo_client, kobo_project_view_uid=kobo_project_view
    ).apply_hints(incremental=last_submission_time_hint)

    kobo_submission = make_resource_kobo_submission(
        kobo_client, kobo_asset, submission_time_start=submission_time_start
    )

    return [kobo_asset, kobo_submission]


def load_kobo(
    submission_time_start: str = "2025-01-01T00:00:00Z",
    asset_last_submission_start: str = "2025-11-01T00:00:01.000Z",
    asset_modified_start: str = "2025-11-01T00:00:01.000Z",
):
    pipeline = dlt.pipeline(
        pipeline_name="klt",
        destination="duckdb",
        dataset_name="klt_dataset",
        pipelines_dir="./dlt_pipelines",
        progress="log",
    )
    pipeline.run(
        kobo_source(
            submission_time_start=submission_time_start,
            asset_last_submission_start=asset_last_submission_start,
            asset_modified_start=asset_modified_start,
        ),
        write_disposition="merge",
    )
