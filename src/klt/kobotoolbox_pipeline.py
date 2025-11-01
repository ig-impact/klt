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
def kobo_source(kobo_token=dlt.secrets.value, kobo_server=dlt.secrets.value, earliest_modified_date="2025-10-20", earliest_submission_date="2025-10-20"):
    config: RESTAPIConfig = {
        "client": kobo_client(kobo_token, kobo_server),
        "resources": [
            res_project_view(selected=False),
            res_asset(
                earliest_modified_date=earliest_modified_date, parallelized=True, selected=False
            ),
            res_asset_content(parallelized=False),  # Maintenant actif pour le logging
            res_submission(earliest_submission_date=earliest_submission_date, parallelized=True),
            # res_audit(),
        ],
    }
    resources = rest_api_resources(config)

    yield from resources


def load_kobo(destination: str = "duckdb", dataset_name: str = "kobo", earliest_modified_date="2025-10-20", earliest_submission_date="2025-10-20"):
    """
    Load Kobo data to specified destination.
    
    Args:
        destination: Destination type ("duckdb" for local, "postgres" for Azure)
        dataset_name: Dataset/schema name in the destination
    """
    pipeline = dlt.pipeline(
        pipeline_name="kobotoolbox_pipeline_azure",
        destination=destination,
        dataset_name=dataset_name,
        pipelines_dir="./dlt_pipelines",
        # staging_dir="./dlt_staging",
        progress="log",
    )

    logger_dlt.info(f"KoboToolbox pipeline run started - Destination: {destination}, Dataset: {dataset_name}")
    load_info = pipeline.run(
        kobo_source(earliest_modified_date=earliest_modified_date, earliest_submission_date=earliest_submission_date),
        write_disposition="replace",
    )
    logger_dlt.info(f"{load_info}")
    last_trace = load_info.last_trace
    pipeline.run([last_trace], table_name = "trace")
    return pipeline
