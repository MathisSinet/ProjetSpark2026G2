# How to use `uv`

`uv` is an extremely fast Python package manager and environment tool.  
It replaces tools like pip, pipx, virtualenv, and pyenv with a single unified workflow.

## Installation

### Linux & macOS
curl -LsSf https://astral.sh/uv/install.sh | sh

Restart your terminal so the `uv` command becomes available.

## Creating and Managing Environments

### Create a virtual environment
uv venv

### Activate the environment
source .venv/bin/activate   # Linux/macOS  
.venv\Scripts\activate      # Windows

## Managing Dependencies

### Add a dependency
uv add requests

### Add a dev dependency
uv add --dev pytest

### Remove a dependency
uv remove requests

### Sync dependencies (lockfile → environment)
uv sync

## Running Code With `uv`

### Run a script inside the environment
uv run script.py

### Run a module
uv run -m mypackage

### Run a command with dependencies (no venv needed)
uv run --with requests python script.py

## Updating Dependencies

### Upgrade a single dependency
uv add --upgrade requests

### Upgrade all dependencies
uv sync --upgrade

## Project Management

### Initialize a new project
uv init

### Create a lockfile without installing
uv lock
