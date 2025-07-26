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
	@$(MAKE) -s api.local-integration-test
	@$(MAKE) -s ui.test
	@$(MAKE) -s sql-format.test
	@echo "Tests completed for all components."

notebook:
	docker run -it --rm -p 8888:8888 \
		-v "${PWD}":/home/jovyan/work \
		quay.io/jupyter/scipy-notebook:latest


# INGESTOR
ingestor.test:
	@echo "####################################################################################################"
	@echo "Running Ingestor tests"
	scripts/run-docker-test.sh ingestor

ingestor.merc-local-run:
	cd ingestor/ && docker buildx build -f Dockerfile -t ingestor .
	docker run --rm --name ingestor \
		-v $(INGESTOR_OUTPUT_PATH):/app/data/ \
		-e TEST_MODE=true \
		ingestor:latest \
		https://tienda.mercadona.es \
		gs://infass-merc/merc

ingestor.carr-local-run:
	cd ingestor/ && docker buildx build -f Dockerfile -t ingestor .
	docker run --rm --name ingestor \
		-v $(INGESTOR_OUTPUT_PATH):/app/data/ \
		-e TEST_MODE=true \
		ingestor:latest \
		https://www.carrefour.es/supermercado \
		gs://infass-carr/carr


# TRANSFORMER
transformer.test:
	@echo "####################################################################################################"
	@echo "Running Transformer tests"
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


# DBT
dbt.test: dbt.deps
	@echo "####################################################################################################"
	@echo "Running DBT tests"
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
	@echo "####################################################################################################"
	@echo "Running API tests"
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
	scripts/test-api-locally.sh
	docker stop infass-api


# UI
ui.test:
	@echo "####################################################################################################"
	@echo "Running UI tests"
	scripts/run-docker-test.sh ui

ui.run:
	cd ui/ && docker buildx build -t ui .
	docker run -p 3000:3000 --rm \
		-e API_BASE_URL=http://host.docker.internal:8080 \
		-e USE_API_MOCKS=false \
		ui:latest



# SQL Formatter
sql-format.run:
	scripts/venv/bin/python3 scripts/format_sql.py

sql-format.test:
	@echo "####################################################################################################"
	@echo "Running SQL format tests"
	scripts/venv/bin/python3 -m unittest scripts/format_sql.py


# Reverse ETL (retl)
retl.test:
	@echo "####################################################################################################"
	@echo "Running Reverse ETL tests"
	scripts/run-docker-test.sh retl

retl.fetch_sqlite:
	retl/venv/bin/python3 retl/fetch_gcs_sqlite.py
