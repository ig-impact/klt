from __future__ import annotations

import pytest

from klt.resources.kobo_asset import (
    date_modified_hint,
    last_submission_time_hint,
    make_resource_kobo_asset,
)


class RESTClientStub:
    """
    - Use .set(*pages) where each page is a list[dict]
    - .paginate(...) yields those pages; extra kwargs are ignored
    """

    def __init__(self):
        self._pages = []

    def set(self, *pages: list[dict]):
        # pages: ([{...}, {...}], [{...}], ...)
        self._pages = list(pages)

    def paginate(self, *, path, params=None, **kwargs):
        for page in self._pages:
            yield page


@pytest.fixture()
def rest_client_stub():
    return RESTClientStub()


@pytest.fixture(scope="function")
def kobo_asset_for_assets_factory(rest_client_stub):
    """
    Factory that builds your real kobo_asset resource with date_modified hint.
    """

    def _build(kobo_project_view_uid: str = "TEST", *, name: str = "kobo_asset"):
        return make_resource_kobo_asset(
            kobo_client=rest_client_stub,
            kobo_project_view_uid=kobo_project_view_uid,
            name=name,
        ).apply_hints(incremental=date_modified_hint)

    return _build


@pytest.fixture(scope="function")
def kobo_asset_for_data_factory(rest_client_stub):
    """
    Factory that builds your real kobo_asset resource with last_submission_time hint.
    """

    def _build(
        kobo_project_view_uid: str = "TEST", *, name: str = "kobo_asset_for_data"
    ):
        return make_resource_kobo_asset(
            kobo_client=rest_client_stub,
            kobo_project_view_uid=kobo_project_view_uid,
            name=name,
        ).apply_hints(incremental=last_submission_time_hint)

    return _build
