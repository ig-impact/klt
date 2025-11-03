from __future__ import annotations

import shutil
from typing import Callable

import dlt
import duckdb
import pytest
from dlt.pipeline import TPipeline


@pytest.fixture(scope="function")
def tmp_pipelines_dir(tmp_path):
    yield tmp_path
    # Clean up any residual pipeline files (state, logs, etc.)
    for child in tmp_path.iterdir():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            try:
                child.unlink(missing_ok=True)
            except Exception:
                pass


@pytest.fixture(scope="function")
def duckdb_conn():
    """Single in-memory DuckDB connection shared by the pipeline *and* test queries."""
    conn = duckdb.connect(":memory:")
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


@pytest.fixture(scope="function")
def pipeline(tmp_pipelines_dir, duckdb_conn):
    """
    dlt pipeline writing to the SAME in-memory connection used for queries.
    """
    p = dlt.pipeline(
        pipeline_name="klt_test",
        destination=dlt.destinations.duckdb(duckdb_conn),
        pipelines_dir=str(tmp_pipelines_dir),
    )
    try:
        yield p
    finally:
        try:
            p.drop()
        except Exception:
            pass


@pytest.fixture(scope="function")
def dataset_name(pipeline: TPipeline) -> str:
    """Schema name where dlt writes tables for this pipeline."""
    return pipeline.dataset_name


@pytest.fixture(scope="function")
def run_pipeline_once(pipeline: TPipeline):
    """
    Execute the pipeline with a resource or list[dict].
    Always pass table_name explicitly: run_pipeline_once(resource, table_name="my_table")
    """
    def _run(data_or_resource, *, table_name: str | None = None, **kwargs):
        if table_name is None:
            return pipeline.run(data_or_resource, **kwargs)          # resource path
        return pipeline.run(data_or_resource, table_name=table_name, **kwargs)  # raw data path
    return _run

@pytest.fixture(scope="function")
def run_twice(run_pipeline_once, rest_client_stub):
    def _run(resource, *, table, first_pages, second_pages, write_disposition="append"):
        # First run
        rest_client_stub.set(*first_pages)
        run_pipeline_once(resource, table_name=table, write_disposition=write_disposition)
        # Second run
        rest_client_stub.set(*second_pages)
        run_pipeline_once(resource, table_name=table, write_disposition=write_disposition)

    return _run

@pytest.fixture(scope="function", autouse=True)
def query(duckdb_conn, dataset_name) -> Callable[[str], list[tuple]]:
    """
    Run ad-hoc SQL against the pipeline's schema on the same connection.
    Usage:
        query(f'SELECT COUNT(*) FROM "{table_name}"')
        query("SHOW TABLES")  # works too
    """
    # Ensure we’re pointing at the pipeline’s schema for unqualified names if needed
    duckdb_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{dataset_name}"')
    duckdb_conn.execute(f"SET schema '{dataset_name}'")

    def _q(sql: str) -> list[tuple]:
        return duckdb_conn.execute(sql).fetchall()

    return _q
