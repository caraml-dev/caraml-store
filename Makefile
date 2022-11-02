PROJECT_ROOT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

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
	--historical-feature-output-location $(GCP_BUCKET_PATH) \
	--store-name $(STORE_NAME) \
	--store-type $(STORE_TYPE)

# Python SDK

PROTOC_IMAGE_VERSION=latest

build-docker-protoc:
	docker build -t protoc:${PROTOC_IMAGE_VERSION} -f caraml-store-python/Dockerfile caraml-store-python

compile-protos-py:
	docker run -v ${PROJECT_ROOT_DIR}:/local protoc

install-python-sdk-local:
	 pip install -e caraml-store-python

package-python-sdk:
	cd caraml-store-python; \
	pip install -r requirements-build.txt; \
	rm -rf build dist; \
	python setup.py sdist bdist_wheel
