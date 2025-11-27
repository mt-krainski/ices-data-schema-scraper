# ICES Data Schema Scraper

A CLI tool for scraping data schema information from ICES (Institute for Clinical Evaluative Sciences) databases.

## Prerequisites

This project uses [uv](https://github.com/astral-sh/uv) as the package manager.

See the [uv installation documentation](https://docs.astral.sh/uv/getting-started/installation/) for installation instructions.

## Installation

The `configure` command will install all necessary dependencies and configure the environment for you.

Run this before first use:

```bash
uv sync
uv run poe configure
```

The `poe configure` command will:

- Install all dependencies
- Set up pre-commit hooks
- Install Playwright browsers

## Usage

The `ices-scraper` command scrapes schema information for a specific library and dataset.

### Basic Usage

```bash
uv run ices-scraper <library_name> <dataset_name>
```

**Arguments:**

- `library_name`: Name of the library to scrape (e.g., `DAD`, `NACRS`, `HCD`)
- `dataset_name`: Full name of the dataset to scrape

**Example:**

```bash
uv run ices-scraper DAD "a. DADyyyy: Discharge Abstract Database -DAD"
```

NOTE: This scraper is pretty slow, it takes around 20 seconds per variable. If you're scraping the schema of a large dataset (e.g. DAD above), it will take a few hours.

### Options

- `--date`, `-d`: Date in ISO format (YYYY-MM-DD). Defaults to today if not provided.
- `--output`, `-o`: Output CSV file path. If not provided, will be auto-generated from library, dataset, and date.
- `--headed`: Run browser in visible mode. By default, runs in headless mode.

### Output

The scraper generates a CSV file containing schema information. The default filename format is:

```
{library_name}__{dataset_name}__{date}.csv
```

## Requirements

- Python >= 3.12
- Playwright (browser automation)
- Typer (CLI framework)

## Development

```bash
# Lint and format
uv run poe lint
uv run poe format
```
