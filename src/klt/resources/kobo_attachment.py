import json
from io import BytesIO

import pandas as pd
from dlt.sources.rest_api.typing import EndpointResource

from ..logging import logger_dlt


def res_audit(
    selected: bool = True,
    parallelized: bool = True,
) -> EndpointResource:
    resource: EndpointResource = {
        "name": "audit",
        "endpoint": {
            "path": "/api/v2/assets/{resources.submission._asset_uid}/data/{resources.submission._id}/attachments/{resources.submission._attachments[*].uid}",
            "data_selector": None,
            "paginator": "single_page",
            "response_actions": [
                prepare_csv,
                {"status_code": 404, "action": "ignore"},
            ],
        },
        "selected": selected,
        "parallelized": parallelized,
        "include_from_parent": ["_asset_uid", "_id", "_uuid"],
    }
    return resource


def prepare_csv(response, *args, **kwargs):
    try:
        csv = pd.read_csv(BytesIO(response.content))
        content = csv.to_json(orient="records")
        response._content = content.encode("utf-8")
        logger_dlt.success(f"Processed audit CSV at {response.url}")
    except Exception as e:
        response._content = json.dumps([]).encode("utf-8")
        logger_dlt.error(f"Failed to process audit CSV: {e} at {response.url}")
    return response