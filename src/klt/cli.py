import typer

from klt.kobotoolbox_pipeline import load_kobo

app = typer.Typer()


@app.command()
def run(
    submission_time_start: str = typer.Option(
        "2025-01-01T00:00:00Z",
        "--submission-time-start",
        help="Initial date for incremental loading of submission data. "
        "Only submissions with _submission_time >= this value will be fetched on first run. "
        "Must be in ISO 8601 format (e.g., 2025-01-01T00:00:00Z).",
    ),
    asset_last_submission_start: str = typer.Option(
        "2025-11-01T00:00:01.000Z",
        "--asset-last-submission-start",
        help="Initial date for filtering assets by deployment__last_submission_time. "
        "Only assets with a last submission >= this value will be processed on first run. "
        "Must be in ISO 8601 format (e.g., 2025-11-01T00:00:01.000Z).",
    ),
    asset_modified_start: str = typer.Option(
        "2025-11-01T00:00:01.000Z",
        "--asset-modified-start",
        help="Initial date for filtering assets by date_modified field. "
        "Only assets modified >= this value will be processed on first run. "
        "Must be in ISO 8601 format (e.g., 2025-11-01T00:00:01.000Z).",
    ),
):
    """
    Run the KoboToolbox data pipeline to extract and load data.

    The pipeline performs incremental loading based on the configured initial dates.
    On subsequent runs, it will automatically resume from the last processed timestamps.
    """
    _ = load_kobo(
        submission_time_start=submission_time_start,
        asset_last_submission_start=asset_last_submission_start,
        asset_modified_start=asset_modified_start,
    )
