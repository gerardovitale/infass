# Makefile

ENV := $(PWD)/.env

include $(ENV)
export


# INGESTOR
ingestor.test:
	cd ingestor/ && docker buildx build -f Dockerfile.test -t ingestor-test .
	docker run --rm ingestor-test:latest

ingestor.run:
	cd ingestor/ && docker buildx build -f Dockerfile -t ingestor .
	docker run --rm --name ingestor \
		-v /Users/gerardovitale/Documents/repos/infass/infass-compute-sa-cred-key.json:/app/keyfile.json \
   		-e GOOGLE_APPLICATION_CREDENTIALS=/app/keyfile.json \
		ingestor:latest


# PANDAS TRANSFORMER
transformer.test:
	cd transformer/ && docker buildx build -f Dockerfile.test -t transformer-test .
	docker run --rm transformer-test:latest

transformer.local-run:
	cd transformer/ && docker buildx build -f Dockerfile -t transformer .
	docker run --rm \
		-v $(TRANSFORMER_OUTPUT_PATH):/app/data/ \
		-v $(GCP_CREDS_PATH):/app/key.json \
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
	cd dbt/ && .dbt_venv/bin/dbt test --profile infass --target dev

dbt.run:
	cd dbt/ && .dbt_venv/bin/dbt run --profile infass --target dev

dbt.build:
	cd dbt/ && .dbt_venv/bin/dbt build --profile infass --target dev


# TERRAFORM
tf-init:
	cd infra/backend_support && terraform init

tf-plan:
	cd infra/backend_support && terraform plan

tf-apply:
	cd infra/backend_support && terraform apply -auto-approve
