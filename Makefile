.PHONY: install gen replay run test lint dq report state lineage bench bench-all docker docker-test clean

install:
	uv venv --python 3.12 .venv || python3 -m venv .venv
	uv pip install -p .venv/bin/python -e ".[dev]" || .venv/bin/pip install -e ".[dev]"

# The source system, with its defects written down in data/source/injected.json.
gen:
	.venv/bin/python -m pw.cli gen

# Fourteen daily runs in order, which is the only way the incremental path and the dimension
# history get exercised the way they would be in production.
replay:
	.venv/bin/python -m pw.cli replay

run:
	.venv/bin/python -m pw.cli run -v

test:
	.venv/bin/python -m pytest -q

lint:
	.venv/bin/ruff check pw bench tests

# Every expectation against the warehouse as it stands. Exits non-zero if an error severity
# check fails, so it works as a gate in a scheduler.
dq:
	.venv/bin/python -m pw.cli test

report:
	.venv/bin/python -m pw.cli report

state:
	.venv/bin/python -m pw.cli state

lineage:
	.venv/bin/python -m pw.cli lineage

# The four measurements the README quotes. cdc_vs_poll builds two warehouses, so it is the slow one.
bench: 
	.venv/bin/python -m bench.incremental
	.venv/bin/python -m bench.sql_rewrite
	.venv/bin/python -m bench.storage
	.venv/bin/python -m bench.extract_threads

bench-all: bench
	.venv/bin/python -m bench.cdc_vs_poll

docker:
	docker build --target runtime -t pipewright:runtime .
	docker run --rm pipewright:runtime report

# The same lint and suite the CI docker job runs, with the network switched off, so a pass here
# means the tests really do not reach for one.
docker-test:
	docker build --target test -t pipewright:test .
	docker run --rm --network none pipewright:test ruff check pw bench tests
	docker run --rm --network none pipewright:test pytest -q

clean:
	rm -rf data .pytest_cache .ruff_cache **/__pycache__
