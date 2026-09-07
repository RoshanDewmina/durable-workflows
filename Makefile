.PHONY: setup test lint demo benchmark serve

setup:
	uv sync --frozen

test:
	uv run ruff check .
	uv run pytest

lint:
	uv run ruff check .

serve:
	uv run durable-workflows

demo:
	uv run python scripts/demo_http.py

benchmark:
	uv run python scripts/benchmark.py --jobs 300 --workers 4 --records-per-job 12 --output evidence/benchmark.json

