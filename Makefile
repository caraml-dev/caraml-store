format-java:
	./gradlew spotlessApply

lint-java:
	./gradlew spotlessCheck

compile:
	./gradlew clean compileJava

test-all:
	./gradlew test

image:
	./gradlew jib

setup-e2e-tests:
	pip install -r e2e_tests/requirements.txt && \
	pip install feast==$(PYTHON_SDK_VERSION) feast-spark==$(SPARK_SDK_VERSION)

run-e2e-tests: setup-e2e-tests
	cd e2e_tests; \
	pytest --verbose \
	--color=yes \
	--registry-url $(CARAML_STORE_REGISTRY_URL) \
	--serving-url $(CARAML_STORE_SERVING_URL) \
	--kafka-brokers $(KAFKA_BROKERS) \
	--bq-project $(GCP_PROJECT) \
	--historical-feature-output-location $(GCP_BUCKET_PATH)
