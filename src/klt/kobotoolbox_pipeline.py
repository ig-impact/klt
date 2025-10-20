import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import ClientConfig, RESTAPIConfig
from rich import print

from .resources import res_asset, res_project_view


def kobo_client(kobo_token: str, kobo_server: str) -> ClientConfig:
    client_config: ClientConfig = {
        "base_url": kobo_server,
        "auth": {
            "type": "api_key",
            "name": "Authorization",
            "api_key": f"Token {kobo_token}",
            "location": "header",
        },
    }
    return client_config


@dlt.source
def kobo_source(kobo_token=dlt.secrets.value, kobo_server=dlt.secrets.value):
    config: RESTAPIConfig = {
        "client": kobo_client(kobo_token, kobo_server),
        "resources": [
            res_project_view(),
            res_asset(),
        ],
    }
    resources = rest_api_resources(config)

    yield from resources


def load_kobo():
    pipeline = dlt.pipeline(
        pipeline_name="kobo",
        destination="duckdb",
        pipelines_dir="./dlt_pipelines",
    )

    load_info = pipeline.run(
        kobo_source(),
        write_disposition="merge",
    )
    print(load_info)
    return pipeline
