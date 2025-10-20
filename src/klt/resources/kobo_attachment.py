import json
from io import BytesIO

import pandas as pd
from dlt.sources.rest_api.typing import EndpointResource

from ..logging import create_http_logger

# Create resource-specific logger
log_audit_http = create_http_logger(resource_name="audit")


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
                log_audit_http,
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
    if response.headers.get("Content-Type") == "text/csv":
        try:
            csv = pd.read_csv(BytesIO(response.content))
            content = csv.to_json(orient="records")
            response._content = content.encode("utf-8")
        except Exception as e:
            e
            __import__("ipdb").set_trace()
    else:
        response._content = json.dumps([]).encode("utf-8")
    return response
