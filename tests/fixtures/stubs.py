from __future__ import annotations


import dlt
import pytest

from klt.resources.kobo_asset import make_resource_kobo_asset


class RESTClientStub:
    """
    - Use .set(*pages) where each page is a list[dict]
    - .paginate(...) yields those pages; extra kwargs are ignored
    - Use .set_for_path(pattern, *pages) for path-specific responses
    """

    def __init__(self):
        self._pages = []
        self._path_responses = {}  # path pattern -> pages

    def set(self, *pages: list[dict]):
        # pages: ([{...}, {...}], [{...}], ...)
        self._pages = list(pages)

    def set_for_path(self, path_pattern: str, *pages: list[dict]):
        """
        Set pages for specific path pattern.

        Usage:
            stub.set_for_path("project-views", [asset1, asset2])
            stub.set_for_path("assets/uid-1/data", [submission1], [submission2])
        """
        self._path_responses[path_pattern] = list(pages)

    def paginate(self, *, path, params=None, **kwargs):
        """
        Simulate KoboToolbox API pagination with server-side filtering.

        Parses query params for _submission_time filtering and applies it
        to returned pages, mimicking the real API behavior.
        """
        import json as json_module

        # Extract cursor from params if present
        cursor_value = None
        if params and "query" in params:
            try:
                query = json_module.loads(params["query"])
                if "_submission_time" in query and "$gte" in query["_submission_time"]:
                    cursor_value = query["_submission_time"]["$gte"]
            except (json_module.JSONDecodeError, KeyError, TypeError):
                pass

        # Check if path matches any pattern
        for pattern, pages in self._path_responses.items():
            if pattern in path:
                for page in pages:
                    if cursor_value:
                        # Filter submissions by _submission_time >= cursor
                        filtered_page = [
                            item
                            for item in page
                            if item.get("_submission_time", "") >= cursor_value
                        ]
                        yield filtered_page
                    else:
                        yield page
                return

        # Fall back to default pages
        for page in self._pages:
            if cursor_value:
                # Filter submissions by _submission_time >= cursor
                filtered_page = [
                    item
                    for item in page
                    if item.get("_submission_time", "") >= cursor_value
                ]
                yield filtered_page
            else:
                yield page


@pytest.fixture()
def rest_client_stub():
    return RESTClientStub()


@pytest.fixture(scope="function")
def kobo_asset_factory(rest_client_stub):
    """
    Universal factory for kobo_asset resources with full control.

    Usage:
        # No incremental hint
        resource = kobo_asset_factory()

        # With date_modified hint
        resource = kobo_asset_factory(hint=date_modified_hint)

        # With parallelization disabled
        resource = kobo_asset_factory(hint=date_modified_hint, parallelized=False)

        # Custom name and project view
        resource = kobo_asset_factory(
            hint=last_submission_time_hint,
            name="my_asset",
            kobo_project_view_uid="ABC123"
        )
    """

    def _build(
        hint=None,
        parallelized=True,
        kobo_project_view_uid="TEST",
        name="kobo_asset",
    ):
        resource = make_resource_kobo_asset(
            kobo_client=rest_client_stub,
            kobo_project_view_uid=kobo_project_view_uid,
            name=name,
            parallelized=parallelized,
        )
        if hint is not None:
            resource = resource.apply_hints(incremental=hint)
        return resource

    return _build


@pytest.fixture(
    params=[
        (
            "date_modified",
            dlt.sources.incremental(
                cursor_path="date_modified",
                initial_value="2025-11-01T00:00:01.000Z",
                on_cursor_value_missing="raise",
            ),
            "raise",
        ),
        (
            "deployment__last_submission_time",
            dlt.sources.incremental(
                cursor_path="deployment__last_submission_time",
                initial_value="2025-11-01T00:00:01.000Z",
                on_cursor_value_missing="include",
            ),
            "include",
        ),
    ],
    ids=["date_modified", "last_submission_time"],
)
def cursor_config(request):
    """
    Parametrize tests over both cursor types.

    Returns tuple of (cursor_field_name, hint, on_cursor_value_missing_behavior).
    The cursor_field_name matches the actual field path used in the hint.

    Usage:
        def test_something(cursor_config, kobo_asset_factory):
            cursor_field, hint, on_missing = cursor_config
            resource = kobo_asset_factory(hint=hint)
            # Test runs twice: once per cursor type
    """
    return request.param


@pytest.fixture(params=[True, False], ids=["parallel", "serial"])
def parallelized(request):
    """
    Parametrize tests over parallelized=True/False.

    Usage:
        def test_something(parallelized, kobo_asset_factory):
            resource = kobo_asset_factory(parallelized=parallelized)
            # Test runs twice: once for parallel, once for serial
    """
    return request.param


@pytest.fixture(
    params=[
        (
            dlt.sources.incremental(
                cursor_path="date_modified",
                initial_value="2025-11-01T00:00:01.000Z",
                on_cursor_value_missing="raise",
            ),
            "date_modified",
            "raise",
            True,
        ),
        (
            dlt.sources.incremental(
                cursor_path="date_modified",
                initial_value="2025-11-01T00:00:01.000Z",
                on_cursor_value_missing="raise",
            ),
            "date_modified",
            "raise",
            False,
        ),
        (
            dlt.sources.incremental(
                cursor_path="deployment__last_submission_time",
                initial_value="2025-11-01T00:00:01.000Z",
                on_cursor_value_missing="include",
            ),
            "deployment__last_submission_time",
            "include",
            True,
        ),
        (
            dlt.sources.incremental(
                cursor_path="deployment__last_submission_time",
                initial_value="2025-11-01T00:00:01.000Z",
                on_cursor_value_missing="include",
            ),
            "deployment__last_submission_time",
            "include",
            False,
        ),
    ],
    ids=[
        "date_modified-parallel",
        "date_modified-serial",
        "last_submission_time-parallel",
        "last_submission_time-serial",
    ],
)
def kobo_asset_all_configs(request, rest_client_stub):
    """
    Parametrized resource testing ALL combinations of cursor type and parallelization.

    Returns tuple of (resource, cursor_field, on_cursor_value_missing, parallelized).

    Usage:
        def test_edge_case(kobo_asset_all_configs):
            resource, cursor_field, on_missing, parallelized = kobo_asset_all_configs
            # Test automatically runs 4 times with all combinations

    Use this fixture when you want comprehensive coverage across all configurations.
    Use kobo_asset_factory + cursor_config/parallelized for more control.
    """
    hint, cursor_field, on_missing, parallelized_val = request.param

    # Generate unique table name based on config
    name_suffix = f"{cursor_field.replace('deployment__', '')}_{parallelized_val}"

    resource = make_resource_kobo_asset(
        kobo_client=rest_client_stub,
        kobo_project_view_uid="TEST",
        name=f"asset_{name_suffix}",
        parallelized=parallelized_val,
    ).apply_hints(incremental=hint)

    return resource, cursor_field, on_missing, parallelized_val


@pytest.fixture(scope="function")
def kobo_submission_factory(rest_client_stub, kobo_asset_factory):
    """
    Factory for kobo_submission resources.

    Usage:
        # Default: creates own asset resource
        resource = kobo_submission_factory()

        # With custom asset
        asset = kobo_asset_factory(hint=some_hint)
        resource = kobo_submission_factory(asset=asset)

        # With custom submission_time_start
        resource = kobo_submission_factory(
            submission_time_start="2025-02-01T00:00:00Z"
        )
    """

    def _build(asset=None, submission_time_start="2025-01-01T00:00:00Z"):
        if asset is None:
            # Create a default asset resource with no incremental hint
            # This ensures we get all assets without filtering
            asset = kobo_asset_factory(hint=None)

        from klt.resources.kobo_submission import make_resource_kobo_submission

        return make_resource_kobo_submission(
            kobo_client=rest_client_stub,
            kobo_asset=asset,
            submission_time_start=submission_time_start,
        )

    return _build
