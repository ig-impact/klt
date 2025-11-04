from klt.resources.kobo_asset import (
    date_modified_hint,
    last_submission_time_hint,
    make_resource_kobo_asset,
)


def test_date_modified_equality_is_loaded_greater_is_loaded(
    run_twice, query, rest_client_stub
):
    base = make_resource_kobo_asset(
        rest_client_stub,
        kobo_project_view_uid="TEST",
        name="kobo_asset_dm_boundary",
    )
    resource = base.apply_hints(incremental=date_modified_hint)

    table = "cursor_dm_boundary"

    first_response = [
        [
            {
                "uid": "x",
                "deployment__submission_count": 1,
                "date_modified": "2025-11-01T00:00:02.000Z",
            },
        ]
    ]
    second_response = [
        [
            {
                "uid": "eq",
                "deployment__submission_count": 1,
                "date_modified": "2025-11-01T00:00:02.000Z",
            },  # equal → load
            {
                "uid": "gt",
                "deployment__submission_count": 1,
                "date_modified": "2025-11-01T00:00:02.001Z",
            },  # greater → load
        ]
    ]

    run_twice(
        resource, table=table, first_pages=first_response, second_pages=second_response
    )

    rows = query(f'SELECT uid, date_modified FROM "{table}" ORDER BY uid')
    assert [r[0] for r in rows] == ["eq", "gt", "x"]


def test_last_submission_time_equality_is_loaded_greater_is_loaded(
    run_twice, query, rest_client_stub
):
    base = make_resource_kobo_asset(
        rest_client_stub,
        kobo_project_view_uid="TEST",
        name="kobo_asset_lst_boundary",
    )
    resource = base.apply_hints(incremental=last_submission_time_hint)

    table = "cursor_lst_boundary"

    first_response = [
        [
            {
                "uid": "x",
                "deployment__submission_count": 1,
                "deployment__last_submission_time": "2025-11-01T00:00:02.000Z",
            },
        ]
    ]
    second_response = [
        [
            {
                "uid": "eq",
                "deployment__submission_count": 1,
                "deployment__last_submission_time": "2025-11-01T00:00:02.000Z",
            },  # equal → load
            {
                "uid": "gt",
                "deployment__submission_count": 1,
                "deployment__last_submission_time": "2025-11-01T00:00:02.001Z",
            },  # greater → load
        ]
    ]

    run_twice(
        resource, table=table, first_pages=first_response, second_pages=second_response
    )

    rows = query(
        f'SELECT uid, deployment__last_submission_time FROM "{table}" ORDER BY uid'
    )
    assert [r[0] for r in rows] == ["eq", "gt", "x"]


def test_date_modified_order_indifferent(run_twice, query, rest_client_stub):
    base = make_resource_kobo_asset(
        rest_client_stub,
        kobo_project_view_uid="TEST",
        name="kobo_asset_dm_boundary",
    )
    resource = base.apply_hints(incremental=date_modified_hint)

    table = "cursor_dm_boundary"

    first_response = [
        [
            {
                "uid": "eq",
                "deployment__submission_count": 1,
                "date_modified": "2025-12-01T00:00:02.000Z",
            },
            {
                "uid": "x",
                "deployment__submission_count": 1,
                "date_modified": "2025-11-01T00:00:02.000Z",
            },
        ]
    ]
    second_response = [
        [
            {
                "uid": "gt",
                "deployment__submission_count": 1,
                "date_modified": "2025-12-02T00:00:02.001Z",
            },  # greater → load
        ]
    ]

    run_twice(
        resource, table=table, first_pages=first_response, second_pages=second_response
    )

    rows = query(f'SELECT uid, date_modified FROM "{table}" ORDER BY uid')
    assert [r[0] for r in rows] == ["eq", "gt", "x"]


def test_no_duplicates(run_twice, query, rest_client_stub):
    base = make_resource_kobo_asset(
        rest_client_stub,
        kobo_project_view_uid="TEST",
        name="kobo_asset_dm_boundary",
    )
    resource = base.apply_hints(incremental=date_modified_hint)

    table = "cursor_dm_boundary"

    first_response = [
        [
            {
                "uid": "eq",
                "deployment__submission_count": 1,
                "date_modified": "2025-12-01T00:00:02.000Z",
            },
            {
                "uid": "x",
                "deployment__submission_count": 1,
                "date_modified": "2025-11-01T00:00:02.000Z",
            },
        ]
    ]

    run_twice(
        resource, table=table, first_pages=first_response, second_pages=first_response
    )

    rows = query(f'SELECT uid, date_modified FROM "{table}" ORDER BY uid')
    assert [r[0] for r in rows] == ["eq", "x"]
