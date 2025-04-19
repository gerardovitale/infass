# Makefile

ENV := $(PWD)/.env

include $(ENV)
export


# INGESTOR
selenium-ingestor.test:
	cd selenium-ingestor/ && docker buildx build -f Dockerfile.test -t selenium-ingestor-test .
	docker run --rm selenium-ingestor-test:latest

selenium-ingestor.run:
	cd selenium-ingestor/ && docker buildx build -f Dockerfile -t selenium-ingestor .
	docker run --rm --name selenium-ingestor \
		-v /Users/gerardovitale/Documents/repos/infass/infass-compute-sa-cred-key.json:/app/keyfile.json \
   		-e GOOGLE_APPLICATION_CREDENTIALS=/app/keyfile.json \
		selenium-ingestor:latest


# PANDAS TRANSFORMER
pd-transformer.test:
	cd pandas-transformer/ && docker buildx build -f Dockerfile.test -t pandas-transformer-test .
	docker run --rm pandas-transformer-test:latest

pd-transformer.local-run:
	cd pandas-transformer/ && docker buildx build -f Dockerfile -t pandas-transformer .
	docker run --rm \
		-v $(TRANSFORMER_OUTPUT_PATH):/app/data/ \
		-v $(GCP_CREDS_PATH):/app/key.json \
		-e GOOGLE_APPLICATION_CREDENTIALS=/app/key.json \
		-e DATA_SOURCE=infass-merc \
		-e DESTINATION=$(GCP_PROJECT_ID).infass.merc \
		-e TRANSFORMER_LIMIT=7 \
		-e IS_LOCAL_RUN=true \
		pandas-transformer:latest


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
	cd tf-infra/backend_support && terraform init

tf-plan:
	cd tf-infra/backend_support && terraform plan

tf-apply:
	cd tf-infra/backend_support && terraform apply -auto-approve
