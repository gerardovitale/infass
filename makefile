selenium-ingestor.test:
	cd selenium-ingestor/ && docker buildx build -f Dockerfile.test -t selenium-ingestor-tests .
	docker run --rm selenium-ingestor-tests:latest

selenium-ingestor.run:
	cd selenium-ingestor/ && docker buildx build -f Dockerfile -t selenium-ingestor .
	docker run --rm --name selenium-ingestor selenium-ingestor:latest
	docker logs -f selenium-ingestor 

spark-jobs.test:
	cd spark-jobs/ && docker buildx build -f Dockerfile.test -t spark-job-tests .
	docker run --rm spark-job-tests:latest
