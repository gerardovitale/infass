cloud-run.test:
	cd cloud-run/ && docker buildx build -f Dockerfile.test -t cloud-run-tests .
	docker run --rm cloud-run-tests:latest

spark-jobs.test:
	cd spark-jobs/ && docker buildx build -f Dockerfile.test -t spark-job-tests .
	docker run --rm spark-job-tests:latest
