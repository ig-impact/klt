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
            return pipeline.run(data_or_resource, **kwargs)  # resource path
        return pipeline.run(
            data_or_resource, table_name=table_name, **kwargs
        )  # raw data path

    return _run


@pytest.fixture(scope="function")
def run_twice(run_pipeline_once, rest_client_stub):
    def _run(resource, *, table, first_pages, second_pages, write_disposition="append"):
        # First run
        rest_client_stub.set(*first_pages)
        run_pipeline_once(
            resource, table_name=table, write_disposition=write_disposition
        )
        # Second run
        rest_client_stub.set(*second_pages)
        run_pipeline_once(
            resource, table_name=table, write_disposition=write_disposition
        )

    return _run


@pytest.fixture(scope="function", autouse=True)
def query(duckdb_conn, dataset_name) -> Callable[[str], list[tuple]]:
    """
    Run ad-hoc SQL against the pipeline's schema on the same connection.
    Usage:
        query(f'SELECT COUNT(*) FROM "{table_name}"')
        query("SHOW TABLES")  # works too
    """
    # Ensure we're pointing at the pipeline's schema for unqualified names if needed
    duckdb_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{dataset_name}"')
    duckdb_conn.execute(f"SET schema '{dataset_name}'")

    def _q(sql: str) -> list[tuple]:
        return duckdb_conn.execute(sql).fetchall()

    return _q


@pytest.fixture(scope="function")
def assert_table_count(query):
    """
    Assert exact row count in a table.
    Usage:
        assert_table_count("my_table", 5)
    """

    def _assert(table_name: str, expected: int):
        [(actual,)] = query(f'SELECT COUNT(*) FROM "{table_name}"')
        assert actual == expected, f"Expected {expected} rows, got {actual}"

    return _assert


@pytest.fixture(scope="function")
def get_table_uids(query):
    """
    Get all UIDs from a table, ordered by uid.
    Usage:
        uids = get_table_uids("my_table")
        assert uids == ["a", "b", "c"]
    """

    def _get(table_name: str) -> list[str]:
        rows = query(f'SELECT uid FROM "{table_name}" ORDER BY uid')
        return [r[0] for r in rows]

    return _get


@pytest.fixture(scope="function")
def get_table_column(query):
    """
    Get all values from a single column, with optional ordering.
    Usage:
        values = get_table_column("my_table", "uid", order_by="uid")
        assert values == ["a", "b", "c"]
    """

    def _get(table_name: str, column: str, *, order_by: str | None = None) -> list:
        sql = f'SELECT {column} FROM "{table_name}"'
        if order_by:
            sql += f" ORDER BY {order_by}"
        rows = query(sql)
        return [r[0] for r in rows]

    return _get
