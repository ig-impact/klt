import dlt
from dlt.sources.helpers.rest_client.client import RESTClient

from klt.resources import make_resource_kobo_asset, make_resource_kobo_submission
from klt.rest_client import make_rest_client


@dlt.source
def kobo_source(
    kobo_token: str = dlt.secrets.value,
    kobo_server: str = dlt.secrets.value,
    kobo_project_view: str = dlt.secrets.value,
):
    kobo_client: RESTClient = make_rest_client(kobo_token, kobo_server)

    kobo_asset = make_resource_kobo_asset(
        kobo_client, kobo_project_view_uid=kobo_project_view
    )

    kobo_submission = make_resource_kobo_submission(kobo_client, kobo_asset)

    return [kobo_asset, kobo_submission]


def load_kobo():
    pipeline = dlt.pipeline(
        pipeline_name="klt",
        destination="duckdb",
        dataset_name="klt_dataset",
        pipelines_dir="./dlt_pipelines",
        progress="log",
    )
    pipeline.run(
        kobo_source(),
        write_disposition="merge",
    )
