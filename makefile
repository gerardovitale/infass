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


# SPARK JOBS
spark-jobs.test:
	cd spark-jobs/ && docker buildx build -f Dockerfile.test -t spark-job-tests .
	docker run --rm spark-job-tests:latest


# TERRAFORM
tf-plan:
	cd tf-infra/ && terraform plan

tf-apply:
	cd tf-infra/ && terraform apply -auto-approve
