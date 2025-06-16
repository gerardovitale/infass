# Makefile

ENV := $(PWD)/.env

include $(ENV)
export

setup:
	scripts/setup-venv.sh

test-all:
	@echo "Running tests for all components..."
	@$(MAKE) -s ingestor.test
	@$(MAKE) -s transformer.test
	@$(MAKE) -s dbt.test
	@$(MAKE) -s api.test
	@$(MAKE) -s ui.test
	@$(MAKE) -s sql-format.test
	@echo "Tests completed for all components."

notebook:
	docker run -it --rm -p 8888:8888 \
		-v "${PWD}":/home/jovyan/work \
		quay.io/jupyter/scipy-notebook:latest


# INGESTOR
ingestor.test:
	scripts/run-docker-test.sh ingestor

ingestor.local-run:
	cd ingestor/ && docker buildx build -f Dockerfile -t ingestor .
	docker run --rm --name ingestor \
		-v $(INGESTOR_OUTPUT_PATH):/app/data/ \
   		-e TEST_MODE=true \
		ingestor:latest


# TRANSFORMER
transformer.test:
	scripts/run-docker-test.sh transformer

transformer.local-run:
	cd transformer/ && docker buildx build -f Dockerfile -t transformer .
	docker run --rm \
		-v $(TRANSFORMER_OUTPUT_PATH):/app/data/ \
		-v $(GCP_TRANSFORMER_CREDS_PATH):/app/key.json \
		-e GOOGLE_APPLICATION_CREDENTIALS=/app/key.json \
		-e DATA_SOURCE=infass-merc \
		-e DESTINATION=$(GCP_PROJECT_ID).infass.merc \
		-e LIMIT=7 \
		-e IS_LOCAL_RUN=true \
		transformer:latest


# SPARK JOBS
spark-jobs.test:
	cd spark-jobs/ && docker buildx build -f Dockerfile.test -t spark-job-test .
	docker run --rm spark-job-test:latest


# DBT
dbt.test: dbt.deps
	cd dbt/ && venv/bin/dbt test --profile infass --target dev

dbt.build: dbt.deps
	cd dbt/ && venv/bin/dbt build --profile infass --target dev

dbt.deps:
	cd dbt/ && venv/bin/dbt deps

dbt.clean:
	cd dbt/ && venv/bin/dbt clean


# TERRAFORM
tf-init:
	cd infra/backend_support && terraform init

tf-plan:
	cd infra/backend_support && terraform plan

tf-apply:
	cd infra/backend_support && terraform apply -auto-approve


# API
api.test:
	scripts/run-docker-test.sh api

api.run:
	cd api/ && docker buildx build -t api .
	docker run --rm -p 8080:8080 \
		-e SQLITE_DB_PATH=/mnt/sqlite/infass-sqlite-api.db \
		-v $(SQLITE_DB_LOCAL_PATH):/mnt/sqlite/infass-sqlite-api.db \
		api:latest

api.local-integration-test:
	cd api/ && docker buildx build -t api .
	docker run -d --rm -p 8080:8080 \
		--name infass-api \
		-e SQLITE_DB_PATH=/mnt/sqlite/infass-sqlite-api.db \
		-v $(SQLITE_DB_LOCAL_PATH):/mnt/sqlite/infass-sqlite-api.db \
		api:latest
	sleep 5
	scripts/test_api_locally.sh
	docker stop infass-api


# UI
ui.test:
	scripts/run-docker-test.sh infass-ui

ui.run:
	cd infass-ui/ && docker buildx build -t infass-ui .
	docker run -p 3000:3000 --rm infass-ui:latest


# SQL Formatter
sql-format.run:
	python3 scripts/format_sql.py

sql-format.test:
	python3 -m unittest scripts/format_sql.py


# Reverse ETL (retl)
retl.test:
	scripts/run-docker-test.sh retl
