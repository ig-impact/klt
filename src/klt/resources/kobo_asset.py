import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.rest_api.typing import EndpointResource


def make_resource_kobo_asset(
    kobo_client: RESTClient,
    kobo_project_view: str,
    earliest_modification_date: str = "2025-10-23",
    page_size: int = 5000,
):
    @dlt.resource()
    def kobo_asset(
        date_modified=dlt.sources.incremental(
            "date_modified", initial_value=earliest_modification_date
        ),
    ):
        path = f"/api/v2/project-views/{kobo_project_view}/assets/"
        params = {
            "format": "json",
            "q": f"date_modified__gte:{date_modified.last_value}",
            "limit": page_size,
        }
        yield kobo_client.paginate(
            path=path,
            params=params,
        )

    kobo_asset.add_filter(lambda r: r.get("has_deployment") is True)
    return kobo_asset


def res_asset_content(
    selected: bool = True,
    parallelized: bool = True,
) -> EndpointResource:
    resource: EndpointResource = {
        "name": "asset_content",
        "endpoint": {
            "path": "/api/v2/assets/{resources.asset.uid}/content/",
            "response_actions": [{"status_code": 400, "action": "ignore"}],
            "paginator": {"type": "json_link", "next_url_path": "next"},
            "data_selector": "data",
        },
        "include_from_parent": ["uid"],
        "parallelized": parallelized,
        "selected": selected,
        "primary_key": "_asset_uid",
    }
    return resource
