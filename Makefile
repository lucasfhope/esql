.PHONY: install install-dev build test

# The 'run' target executes your Python script with poetry.

install :; poetry install --no-dev

install-dev :; poetry install

build :; poetry build

test :; poetry run pytest -vv