.PHONY: install test lint fmt demo run worker chaos chaos-small replay screenshot docker up down clean

PORT ?= 8823
DB ?= data/demo.db

install:
	uv sync --python 3.12

test:
	uv run --python 3.12 --group dev pytest -q

lint:
	uvx ruff check .

fmt:
	uvx ruff check --fix .

demo:
	uv run durableflow demo --db $(DB)

run: ## server with one embedded worker, UI at http://127.0.0.1:8823
	uv run durableflow server --db $(DB) --workers 1 --port $(PORT)

worker:
	uv run durableflow worker --db $(DB)

chaos: ## 200 workflows, 4 worker processes, kills and faults, then invariants
	uv run durableflow chaos --workflows 200 --workers 4 --seed 7 --out docs/chaos-200.txt

chaos-small:
	uv run durableflow chaos --workflows 40 --workers 3 --seed 7 --kill-every 1.0

replay: ## replay every stored history against the current code
	uv run durableflow replay --db $(DB)

screenshot: ## needs make demo and make run first, and Google Chrome
	uv run python scripts/screenshot.py --port $(PORT)

docker:
	docker build -t durableflow:local .

up:
	docker compose up --build -d

down:
	docker compose down

clean:
	rm -rf data .pytest_cache .ruff_cache
