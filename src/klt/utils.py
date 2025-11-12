from typing import Any

import pendulum
from dlt.sources.rest_api.config_setup import create_response_hooks
from dlt.sources.rest_api.typing import ResponseAction

from .logging import http_log, logger


def make_kobo_pipeline_hooks(
    response_actions: list[ResponseAction] | None = None,
    ignored_http_status_codes: list[int] | None = None,
    enable_http_logging: bool = True,
):
    response_actions = response_actions or []
    ignored_http_status_codes = ignored_http_status_codes or []
    _response_actions = []
    if enable_http_logging:
        _response_actions.append(http_log)
    for http_status_code in ignored_http_status_codes:
        _response_actions.append(
            {"status_code": http_status_code, "action": "ignore"},
        )
    return create_response_hooks([*_response_actions, *response_actions])


def parse_timestamps(item: dict[str, Any]) -> dict[str, Any]:
    """
    Parse timestamp fields in a kobo asset.

    Converts ISO 8601 timestamp strings to pendulum.DateTime objects for common
    Kobo Timestamp fields.

    :param asset: Kobo Asset as a dict with string timestamps
    :return: Kobo Asset with timestamp fields converted to pendulum.DateTime
    """

    timestamp_fields = [
        "deployment__last_submission_time",
        "date_modified",
        "date_created",
        "date_deployed",
        "_submission_time",
    ]
    for timestamp_field in timestamp_fields:
        if timestamp_field in item and item[timestamp_field] is not None:
            try:
                item[timestamp_field] = pendulum.parse(item[timestamp_field])
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Failed to parse {timestamp_field}={item[timestamp_field]!r} "
                    f"in asset {item.get('uid')}: {e}"
                )
    return item
