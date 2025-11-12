from datetime import datetime

import dlt
from dlt.sources.helpers.rest_client.client import RESTClient

from klt.utils import make_kobo_pipeline_hooks, parse_timestamps

asset_hooks = make_kobo_pipeline_hooks(
    ignored_http_status_codes=[404, 502], enable_http_logging=True
)


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
            hooks=asset_hooks,
        ):
            yield from page

    kobo_asset.add_map(parse_timestamps)
    kobo_asset.add_filter(lambda ka: (ka.get("deployment__submission_count") or 0) > 0)
    return kobo_asset


def make_last_submission_time_hint(initial_value: datetime):
    """
    Create incremental hint for deployment__last_submission_time cursor.

    Includes assets missing this field (allows None) since some assets may not have
    deployment__last_submission_time immediately available.
    """
    return dlt.sources.incremental(
        cursor_path="deployment__last_submission_time",
        initial_value=initial_value,
        on_cursor_value_missing="include",
    )


def make_date_modified_hint(initial_value: datetime):
    """
    Create incremental hint for date_modified cursor.

    Raises error on missing cursor value since all assets must have date_modified.
    Currently unused but available for future filtering needs.
    """
    return dlt.sources.incremental(
        cursor_path="date_modified",
        initial_value=initial_value,
        on_cursor_value_missing="raise",
    )
