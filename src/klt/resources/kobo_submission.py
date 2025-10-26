import json

import dlt
from dlt.sources.helpers.rest_client import RESTClient


def make_resource_kobo_submission(
    kobo_client: RESTClient, kobo_asset, earliest_submission_date: str = "2025-10-23"
):
    @dlt.resource(data_from=kobo_asset)
    def kobo_submission(
        assets,
        submission_time=dlt.sources.incremental(
            cursor_path="_submission_time", initial_value=earliest_submission_date
        ),
    ):
        for asset in assets:
            if not asset.get("has_deployment"):
                continue
            asset_uid = asset["uid"]
            path = f"/api/v2/assets/{asset_uid}/data/"
            params = {
                "query": json.dumps(
                    {"_submission_time": {"$gte": submission_time.last_value}}
                ),
                "format": "json",
            }
            yield kobo_client.paginate(
                path=path, params=params, data_selector="results"
            )

    kobo_submission.add_map(transform_submission_data)
    return kobo_submission


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
    val["responses"] = eav
    return val
