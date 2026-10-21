.PHONY: install demo demo-api demo-api-dlt test serve reset
install:
	pip install -r requirements.txt
reset:
	rm -rf data
demo: reset
	python -m lakehouse.generate --start 2024-01-01 --days 1 --no-updates
	python -m lakehouse.pipeline
	@echo "\n===== DAY 2: new file arrives (incremental) =====\n"
	python -m lakehouse.generate --start 2024-01-02 --days 1
	python -m lakehouse.pipeline
	@echo "\n===== RE-RUN with no new data (idempotent no-op) =====\n"
	python -m lakehouse.pipeline
	@echo ""
	python -m lakehouse.show
demo-api: reset
	python scripts/demo_api.py
demo-api-dlt: reset
	EXTRACT_BACKEND=dlt python scripts/demo_api.py
test:
	pytest -v
serve:
	python flows/prefect_flow.py --serve
