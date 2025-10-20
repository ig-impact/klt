import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import ClientConfig, RESTAPIConfig

from .logging import logger_dlt
from .resources import (
    res_asset,
    res_asset_content,
    res_audit,
    res_project_view,
    res_submission,
)


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
            res_project_view(selected=False),
            res_asset(
                earliest_modified_date="2025-10-01", parallelized=True, selected=False
            ),
            res_asset_content(parallelized=False),  # Maintenant actif pour le logging
            res_submission(earliest_submission_date="2025-10-01", parallelized=True),
            res_audit(),
        ],
    }
    resources = rest_api_resources(config)

    yield from resources


def load_kobo():
    pipeline = dlt.pipeline(
        pipeline_name="kobotoolbox_pipeline",
        destination="duckdb",
        dataset_name="kobo",
        pipelines_dir="./dlt_pipelines",
    )

    logger_dlt.info("KoboToolbox pipeline run started")
    load_info = pipeline.run(
        kobo_source(),
        write_disposition="replace",
    )
    logger_dlt.info(f"{load_info}")
    return pipeline
