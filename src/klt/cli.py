import os
import sys
import typer
from klt.kobotoolbox_pipeline import load_kobo

app = typer.Typer()

@app.command()
def run(
    pipeline_name: str = typer.Option(os.getenv("KLT_PIPELINE_NAME", "kobo_data_pipeline")),
    destination: str = typer.Option(os.getenv("KLT_DESTINATION", "postgre")),
    dataset_name: str = typer.Option(os.getenv("KLT_DATASET_NAME", "kobo_2025_historic")),
    earliest_modified_date: str = typer.Option(os.getenv("KLT_EARLIEST_MODIFIED_DATE", "2025-01-01")),
    earliest_submission_date: str = typer.Option(os.getenv("KLT_EARLIEST_SUBMISSION_DATE", "2000-01-01")),
):
    """Run the KoboToolbox ETL pipeline."""
    try:
        load_kobo(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name, earliest_modified_date=earliest_modified_date, earliest_submission_date=earliest_submission_date)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
