.PHONY: setup seed run test docs all

# Requires Python 3.11 or 3.12 (dbt 1.8 is not compatible with Python 3.14+)
# Run: python3.12 -m venv .venv && source .venv/bin/activate
setup:
	pip install "dbt-core==1.8.*" "dbt-duckdb==1.8.*" && dbt deps --profiles-dir .

seed:
	python generate_seeds.py && dbt seed --profiles-dir .

run:
	dbt run --profiles-dir .

test:
	dbt test --profiles-dir .

docs:
	dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

all: setup seed run test

# Selective layer runs
bronze:
	dbt run --select bronze --profiles-dir .

silver:
	dbt run --select silver --profiles-dir .

gold:
	dbt run --select gold --profiles-dir .

platinum:
	dbt run --select platinum --profiles-dir .

# Full lineage from a model
lineage:
	dbt run --select +rpt_monthly_custody_statement --profiles-dir .
