import dlt
from dlt.sources.helpers.rest_client.client import RESTClient

from ..models import ProjectViewAssetResponse


def make_resource_kobo_asset(
    kobo_client: RESTClient,
    kobo_project_view_uid: str,
    page_size: int = 1000,
):
    @dlt.resource(
        name="kobo_asset",
        primary_key=["uid"],
        parallelized=True,
        columns=ProjectViewAssetResponse,
    )
    def kobo_asset(
        latest_submission_time_cursor=dlt.sources.incremental(
            cursor_path="deployment__last_submission_time",
            initial_value="2025-01-01T00:00:00Z",
            on_cursor_value_missing="include",
        ),
    ):
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
        ):
            for asset in page:
                submission_count: int = asset.get("deployment__submission_count") or 0
                if submission_count == 0:
                    continue
                yield asset

    # kobo_asset.add_map(debug_asset)
    return kobo_asset


def debug_asset(asset):
    import ipdb

    ipdb.set_trace()
