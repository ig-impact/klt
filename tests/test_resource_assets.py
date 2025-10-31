from hypothesis import HealthCheck, given, settings

from .conftest import asset_page_strategy, pipeline_ctx


@settings(max_examples=10)
@given(asset_page_strategy())
def test_rules(assets):
    for asset in assets:
        if asset["submission_count"] > 0:
            assert asset["deployment__last_submission_time"] is not None


@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(assets=asset_page_strategy())
def test_pipeline(tmp_path, assets):
    with pipeline_ctx(tmp_path) as pipeline:
        assert pipeline.first_run is True
        table = "klt_test"
        _ = pipeline.run(assets, table_name=table)

        with (
            pipeline.sql_client() as c,
            c.execute_query(
                f"SELECT COUNT(*) FROM {pipeline.dataset_name}.{table}"
            ) as cur,
        ):
            assert cur.fetchone()[0] >= 1
