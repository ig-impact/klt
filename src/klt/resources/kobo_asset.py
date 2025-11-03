import dlt
from dlt.sources.helpers.rest_client.client import RESTClient

from ..logging import http_log


def make_resource_kobo_asset(
    kobo_client: RESTClient,
    kobo_project_view_uid: str,
    name: str = "kobo_asset",
    page_size: int = 5000,
    parallelized: bool = True,
):
    @dlt.resource(
        name=name,
        primary_key=["uid"],
        parallelized=parallelized,
    )
    def kobo_asset():
        path = f"/api/v2/project-views/{kobo_project_view_uid}/assets/"
        params = {
            "format": "json",
            "limit": page_size,
        }
        for page in kobo_client.paginate(
            path=path,
            params=params,
            data_selector="results",
            allow_redirects=False,
            hooks={"response": [http_log]},
        ):
            yield from page

    kobo_asset.add_filter(lambda ka: (ka.get("deployment__submission_count") or 0) > 0)
    return kobo_asset


last_submission_time_hint = dlt.sources.incremental(
    cursor_path="deployment__last_submission_time",
    initial_value="2025-11-01T00:00:01.000Z",
    on_cursor_value_missing="include",
)

date_modified_hint = dlt.sources.incremental(
    cursor_path="date_modified",
    initial_value="2025-11-01T00:00:01.000Z",
    on_cursor_value_missing="raise",
)
