import typer

from klt.kobotoolbox_pipeline import load_kobo

app = typer.Typer()


@app.command()
def run():
    _ = load_kobo()
