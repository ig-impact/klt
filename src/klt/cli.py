import os
import sys
import typer
from klt.kobotoolbox_pipeline import load_kobo

app = typer.Typer()

@app.command()
def run(
    destination: str = typer.Option(os.getenv("KLT_DESTINATION", "postgres")),
    dataset_name: str = typer.Option(os.getenv("KLT_DATASET_NAME", "kobo_data")),
    earliest_modified_date: str = typer.Option(os.getenv("KLT_EARLIEST_MODIFIED_DATE", "2025-10-25")),
    earliest_submission_date: str = typer.Option(os.getenv("KLT_EARLIEST_SUBMISSION_DATE", "2000-10-25")),
):
    """Run the KoboToolbox ETL pipeline."""
    try:
        load_kobo(destination=destination, dataset_name=dataset_name, earliest_modified_date=earliest_modified_date, earliest_submission_date=earliest_submission_date)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
