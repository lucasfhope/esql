.PHONY: run

# The 'run' target executes your Python script with poetry.

install:
	poetry install --no-dev


run:
	poetry run python main.py

build:
	poetry build

test:
	poetry run pytest