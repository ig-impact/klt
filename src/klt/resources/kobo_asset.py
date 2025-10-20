from dlt.sources.rest_api.typing import EndpointResource

from .http_logger import create_http_logger

# Create resource-specific loggers
log_asset_http = create_http_logger(resource_name="asset")
log_asset_content_http = create_http_logger(resource_name="asset_content")


def res_asset(
    page_size: int = 5000,
    earliest_modified_date: str = "2025-08-01",
    selected: bool = True,
    parallelized: bool = True,
) -> EndpointResource:
    resource: EndpointResource = {
        "name": "asset",
        "endpoint": {
            "path": "/api/v2/project-views/{resources.project_view.uid}/assets",
            "data_selector": "results",
            "params": {
                "q": "date_modified__gte:{incremental.start_value}",
                "limit": page_size,
            },
            "incremental": {
                "cursor_path": "date_modified",
                "initial_value": earliest_modified_date,
            },
            "response_actions": [log_asset_http],
            "paginator": {"type": "json_link", "next_url_path": "next"},
        },
        "selected": selected,
        "parallelized": parallelized,
        "primary_key": "uid",
        "processing_steps": [{"filter": lambda r: r.get("has_deployment") is True}],
    }
    return resource


def res_asset_content(
    selected: bool = True,
    parallelized: bool = True,
) -> EndpointResource:
    resource: EndpointResource = {
        "name": "asset_content",
        "endpoint": {
            "path": "/api/v2/assets/{resources.asset.uid}/content/",
            "response_actions": [
                log_asset_content_http,
                {"status_code": 400, "action": "ignore"},
            ],
            "paginator": {"type": "json_link", "next_url_path": "next"},
            "data_selector": "data",
        },
        "include_from_parent": ["uid"],
        "parallelized": parallelized,
        "selected": selected,
        "primary_key": "_asset_uid",
    }
    return resource
