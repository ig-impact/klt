import json
from io import BytesIO

import dlt
import pandas as pd
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

# from .logging import logger
# from .resources import res_asset, res_project_view


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


@dlt.source
def kobo_source(
    kobo_token: str = dlt.secrets.value, kobo_server: str = dlt.secrets.value
):
    kobo_client: RESTClient = RESTClient(
        base_url=kobo_server,
        auth=APIKeyAuth(
            name="Authorization", api_key=f"Token {kobo_token}", location="header"
        ),
        paginator=JSONLinkPaginator(next_url_path="pagination.next"),
    )

    @dlt.resource()
    def kobo_asset(
        kobo_project_view: str = dlt.secrets.value,
        date_modified=dlt.sources.incremental(
            "date_modified", initial_value="2025-10-23"
        ),
    ):
        path = f"/api/v2/project-views/{kobo_project_view}/assets/"
        params = {
            "format": "json",
            "q": f"date_modified__gte:{date_modified.last_value}",
            "limit": 5000,
        }
        yield from kobo_client.paginate(
            path=path,
            params=params,
        )

    @dlt.transformer(name="submission", data_from=kobo_asset)
    def kobo_submission(
        assets,
        submission_time=dlt.sources.incremental(
            cursor_path="_submission_time", initial_value="2025-10-23"
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
            yield from kobo_client.paginate(
                path=path, params=params, data_selector="results"
            )

    kobo_submission.add_map(transform_submission_data)

    @dlt.transformer(name="audit", data_from=kobo_submission)
    def kobo_audit(submissions):
        for submission in submissions:
            audit_file = next(
                filter(
                    lambda s: s.get("media_file_basename") == "audit.csv",
                    submission.get("_attachments", []),
                )
            )
            if audit_file:
                path = audit_file["download_url"]
                path = path.replace("?format=json", "")
                response = kobo_client.get(path)
                response.raise_for_status()
                csv_content = pd.read_csv(BytesIO(response.content))
                if not csv_content.empty:
                    yield from csv_content.to_dict(orient="records")

    return [kobo_asset, kobo_submission, kobo_audit]


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="klt",
        destination="duckdb",
        dataset_name="klt_dataset",
        pipelines_dir="./dlt_pipelines",
        progress="log",
    )
    pipeline.run(
        kobo_source(),
        write_disposition="replace",
    )
