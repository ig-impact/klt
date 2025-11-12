import json
from datetime import datetime

import dlt
import pendulum
from dlt.sources.helpers.rest_client.client import RESTClient

from klt.utils import make_kobo_pipeline_hooks, parse_timestamps

submission_hooks = make_kobo_pipeline_hooks(
    ignored_http_status_codes=[404, 502], enable_http_logging=True
)


def make_resource_kobo_submission(
    kobo_client: RESTClient, kobo_asset, submission_time_start: datetime
):
    @dlt.transformer(
        data_from=kobo_asset,
        parallelized=False,
        name="kobo_submission",
        primary_key=["_id", "_uuid"],
    )
    def kobo_submission(
        asset,
    ):
        asset_uid = asset["uid"]

        path = f"/api/v2/assets/{asset_uid}/data/"
        params = {
            "format": "json",
        }
        for page in kobo_client.paginate(
            path=path, params=params, data_selector="results", hooks=submission_hooks
        ):
            yield from page

    kobo_submission.add_map(parse_timestamps)
    kobo_submission.add_map(transform_submission_data)
    return kobo_submission


def transform_submission_data(data: dict):
    excluded = ["_geolocation", "_downloads", "_validation_status"]
    fields = [key for key in data if str(key).startswith("_") and key not in excluded]
    questions = [key for key in data if key not in fields and key not in excluded]
    data["_submission_time"] = pendulum.parse(data["_submission_time"])
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
