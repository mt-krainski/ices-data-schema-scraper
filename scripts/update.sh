#!/usr/bin/env bash

uv sync --dev
uv run pre-commit autoupdate
