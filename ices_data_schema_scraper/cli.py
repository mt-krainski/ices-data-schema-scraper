"""CLI tool for ICES data schema scraper."""

import asyncio
from datetime import datetime
from typing import Optional

import typer

from ices_data_schema_scraper.scraper import scrape_ices_data

app = typer.Typer()


@app.command()
def scrape(
    library_name: str = typer.Argument(
        ..., help="Name of the library to scrape (e.g., 'DAD')"
    ),
    dataset_name: str = typer.Argument(..., help="Name of the dataset to scrape"),
    date: Optional[str] = typer.Option(
        None,
        "--date",
        "-d",
        help="Date in ISO format (YYYY-MM-DD). Defaults to today if not provided.",
    ),
    output_csv: Optional[str] = typer.Option(
        None,
        "--output",
        "-o",
        help=(
            "Output CSV file path. If not provided, will be generated from "
            "library, dataset, and date."
        ),
    ),
    headed: bool = typer.Option(
        False,
        "--headed",
        help="Run browser in headed mode (visible). By default, runs in headless mode.",
    ),
) -> None:
    """
    Scrape ICES data schema information.

    Examples:
        ices-scraper DAD "a. DADyyyy: Discharge Abstract Database -DAD"
        ices-scraper DAD "a. DADyyyy: Discharge Abstract Database -DAD" \
            --date 2025-01-15
        ices-scraper DAD "a. DADyyyy: Discharge Abstract Database -DAD" \
            -o custom_output.csv
        ices-scraper DAD "a. DADyyyy: Discharge Abstract Database -DAD" --headed
    """
    # Determine the date to use
    if date:
        try:
            date_obj = datetime.fromisoformat(date).date()
        except ValueError as err:
            typer.echo(
                f"Error: Invalid date format '{date}'. Use YYYY-MM-DD format.", err=True
            )
            raise typer.Exit(1) from err
    else:
        date_obj = datetime.now().date()

    date_str = date_obj.isoformat()

    # Generate output CSV filename if not provided
    if output_csv is None:
        # Sanitize dataset name for filename
        safe_dataset_name = dataset_name.replace(" ", "-").replace(":", "-")
        output_csv = f"{library_name}__{safe_dataset_name}__{date_str}.csv"

    typer.echo("Starting scraper with:")
    typer.echo(f"  Library: {library_name}")
    typer.echo(f"  Dataset: {dataset_name}")
    typer.echo(f"  Date: {date_str}")
    typer.echo(f"  Output: {output_csv}")
    typer.echo(f"  Headed mode: {headed}")
    typer.echo()

    # Run the scraper
    asyncio.run(
        scrape_ices_data(
            library_name=library_name,
            dataset_name=dataset_name,
            output_csv=output_csv,
            headed=headed,
        )
    )

    typer.echo()
    typer.echo(f"Scraping completed! Results saved to: {output_csv}")


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
