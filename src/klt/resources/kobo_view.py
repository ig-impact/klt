from dlt.sources.rest_api.typing import EndpointResource

from ..logging import create_http_logger

# Create resource-specific logger
log_project_view_http = create_http_logger(resource_name="project_view")


def res_project_view(
    page_size: int = 10000, selected: bool = False
) -> EndpointResource:
    resource: EndpointResource = {
        "name": "project_view",
        "endpoint": {
            "path": "/api/v2/project-views",
            "data_selector": "results",
            "params": {
                "limit": page_size,
            },
            "response_actions": [log_project_view_http],
            "paginator": "json_link",
        },
        "selected": selected,
        "primary_key": "uid",
    }
    return resource
