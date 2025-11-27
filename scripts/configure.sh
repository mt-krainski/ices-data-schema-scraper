#!/usr/bin/env bash

# This script prepares the development envrionment.
# It installs all relevant plugins, additional packages
# and creates a template .env file.

uv sync --extra dev

uv run pre-commit install
uv run playwright install

cat > .env <<EOL

EOL
