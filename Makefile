.PHONY: install lint test demo

install:
	python -m pip install -e ".[dev]"

lint:
	ruff check .
	ruff format --check .

test:
	pytest

demo:
	python -m andensur demo --orders 120 --seed 42 --fresh
