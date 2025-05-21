# Makefile

ENV := $(PWD)/.env

include $(ENV)
export

setup:
	scripts/setup.sh

test-all:
	@echo "Running tests for all components..."
	@$(MAKE) -s ingestor.test
	@$(MAKE) -s transformer.test
	@$(MAKE) -s dbt.test
	@$(MAKE) -s api.test
	@$(MAKE) -s ui.test
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


# PANDAS TRANSFORMER
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
		-e TRANSFORMER_LIMIT=7 \
		-e IS_LOCAL_RUN=true \
		transformer:latest


# SPARK JOBS
spark-jobs.test:
	cd spark-jobs/ && docker buildx build -f Dockerfile.test -t spark-job-test .
	docker run --rm spark-job-test:latest


# DBT
dbt.test:
	cd dbt/ && venv/bin/dbt test --profile infass --target dev

dbt.build:
	cd dbt/ && venv/bin/dbt build --profile infass --target dev


# TERRAFORM
tf-init:
	cd infra/backend_support && terraform init

tf-plan:
	cd infra/backend_support && terraform plan

tf-apply:
	cd infra/backend_support && terraform apply -auto-approve


# API
api.test:
	scripts/run-docker-test.sh infass-api

api.run:
	cd infass-api/ && docker buildx build -t infass-api .
	docker run -p 8000:8000 --rm infass-api:latest


# UI
ui.test:
	scripts/run-docker-test.sh infass-ui

ui.run:
	cd infass-ui/ && docker buildx build -t infass-ui .
	docker run -p 3000:3000 --rm infass-ui:latest
