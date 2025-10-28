import os
import sys
import typer
from klt.kobotoolbox_pipeline import load_kobo

app = typer.Typer()

@app.command()
def run(
    destination: str = typer.Option(os.getenv("KLT_DESTINATION", "postgres")),
    dataset_name: str = typer.Option(os.getenv("KLT_DATASET_NAME", "kobo_data")),
):
    """Run the KoboToolbox ETL pipeline."""
    try:
        load_kobo(destination=destination, dataset_name=dataset_name)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
