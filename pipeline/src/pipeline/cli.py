"""Typer CLI for the Boston Urban Hazard Maps pipeline."""

import logging
from typing import Annotated

import typer

app = typer.Typer(help="Boston Urban Hazard Maps daily data pipeline.")


@app.command()
def run(
    datasets: Annotated[
        list[str] | None,
        typer.Option("--dataset", "-d", help="Datasets to process (needles, encampments, waste). Repeatable."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Re-fetch all years from CKAN, ignoring cached raw data."),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable debug logging."),
    ] = False,
) -> None:
    """Run the pipeline."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    from pipeline.run import run_pipeline

    counts = run_pipeline(datasets=datasets, force=force)

    total = sum(counts.values())
    typer.echo(f"Done. Processed {total} records across {len(counts)} dataset(s).")
    for name, count in counts.items():
        typer.echo(f"  {name}: {count}")


if __name__ == "__main__":
    app()
