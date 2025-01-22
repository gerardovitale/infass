# Makefile

ENV := $(PWD)/.env

include $(ENV)
export

# INGESTOR
selenium-ingestor.test:
	cd selenium-ingestor/ && docker buildx build -f Dockerfile.test -t selenium-ingestor-tests .
	docker run --rm selenium-ingestor-tests:latest

selenium-ingestor.run:
	cd selenium-ingestor/ && docker buildx build -f Dockerfile -t selenium-ingestor .
	docker run --rm --name selenium-ingestor \
		-v /Users/gerardovitale/Documents/repos/infass/infass-compute-sa-cred-key.json:/app/keyfile.json \
   		-e GOOGLE_APPLICATION_CREDENTIALS=/app/keyfile.json \
		selenium-ingestor:latest


# PANDAS TRANSFORMER
pd-transformer.test:
	cd pandas-transformer/ && docker buildx build -f Dockerfile.test -t pandas-transformer-tests .
	docker run --rm pandas-transformer-tests:latest


# SPARK JOBS
spark-jobs.test:
	cd spark-jobs/ && docker buildx build -f Dockerfile.test -t spark-job-tests .
	docker run --rm spark-job-tests:latest


# TERRAFORM
tf-init:
	cd tf-infra/backend_support && terraform init

tf-plan:
	cd tf-infra/backend_support && terraform plan

tf-apply:
	cd tf-infra/backend_support && terraform apply -auto-approve
