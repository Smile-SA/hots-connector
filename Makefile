.PHONY: lint test format typecheck

lint:
	python -m ruff check .

format:
	python -m ruff format .

typecheck:
	python -m mypy hots_connector

test:
	python -m pytest -q
