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

# SDK

bufbuild-image:
	docker build -f bufbuild.Dockerfile . -t caraml-store-bufbuild:build

bufbuild-proto:
	docker run --volume "${PWD}:/caraml-store" --workdir /caraml-store/caraml-store-protobuf/src/main/proto caraml-store-bufbuild:build generate --template buf.gen.sdk.yaml

compile-protos-py:
	docker run -v ${PROJECT_ROOT_DIR}:/local protoc

install-python-sdk-local:
	 pip install -e caraml-store-sdk/python

package-python-sdk:
	cd caraml-store-sdk/python; \
	pip install -r requirements-build.txt; \
	rm -rf build dist; \
	python setup.py sdist bdist_wheel
