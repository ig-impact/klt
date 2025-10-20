import json

from dlt.sources.rest_api.typing import EndpointResource

from .http_logger import create_http_logger

# Create resource-specific logger
log_submission_http = create_http_logger(resource_name="submission")


def res_submission(
    earliest_submission_date: str = "2025-08-01",
    selected: bool = True,
    parallelized: bool = True,
) -> EndpointResource:
    resource: EndpointResource = {
        "name": "submission",
        "endpoint": {
            "path": "/api/v2/assets/{resources.asset.uid}/data/",
            "incremental": {
                "cursor_path": "_submission_time",
                "initial_value": earliest_submission_date,
            },
            "params": {
                "query": '{{"_submission_time": {{"$gte": "{incremental.start_value}"}}}}',
                "format": "json",
            },
            "data_selector": "results",
            "response_actions": [
                log_submission_http,
                {"status_code": 400, "action": "ignore"},
            ],
            "paginator": {"type": "json_link", "next_url_path": "next"},
        },
        "include_from_parent": ["uid"],
        "primary_key": ["_uuid", "_id"],
        "parallelized": parallelized,
        "selected": selected,
        "processing_steps": [
            {"map": transform_submission_data},
        ],
    }
    return resource


def transform_submission_data(data: dict):
    excluded = ["_geolocation", "_downloads", "_validation_status"]
    fields = [key for key in data if str(key).startswith("_") and key not in excluded]
    questions = [key for key in data if key not in fields and key not in excluded]
    eav = []
    for question in questions:
        if isinstance(data[question], list):
            data[question] = json.dumps(data[question])
        eav.append(
            {
                "question": question,
                "response": data[question],
            }
        )
    val = {key: data[key] for key in fields}
    if "_attachments" in fields:
        if len(data["_attachments"]) == 0:
            val["_attachments"] = [{"uid": "INVALID"}]
    val["responses"] = eav
    return val
