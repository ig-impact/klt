from dlt.sources.rest_api.typing import EndpointResource


def res_project_view(
    page_size: int = 10000, selected: bool = False
) -> EndpointResource:
    resource: EndpointResource = {
        "name": "project_view",
        "endpoint": {
            "path": "/api/v2/project-views/",
            "data_selector": "results",
            "params": {
                "limit": page_size,
            },
            "paginator": "json_link",
        },
        "selected": selected,
        "primary_key": "uid",
    }
    return resource
