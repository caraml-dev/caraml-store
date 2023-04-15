#!/usr/bin/env bash

if (($# != 1))
then
  echo "Bad input !! Pass path to proto package as argument"
  echo "eg. $0 /app/caraml-store/caraml-store-protobuf"
  exit 1
fi

PROTOS_PACKAGE_PATH=$1
PROTOS_DIR="${PROTOS_PACKAGE_PATH}/src/main/proto"

cd "${PROTOS_DIR}" || { echo "Proto directory - ${PROTOS_DIR} doesn't exist !!"; exit 2; }

OUT_DIR=${PROTOS_PACKAGE_PATH}/../caraml-store-sdk/python
mkdir -p "${OUT_DIR}"

PROTO_TYPE_SUBDIRS="core serving types"
for dir in ${PROTO_TYPE_SUBDIRS}
do
  python -m grpc_tools.protoc -I. --python_out="${OUT_DIR}" --mypy_out="${OUT_DIR}" feast/"${dir}"/*.proto;
done

PROTO_SERVICE_SUBDIRS="core serving"
for dir in ${PROTO_SERVICE_SUBDIRS}
do
  python -m grpc_tools.protoc -I. --grpc_python_out="${OUT_DIR}" feast/"${dir}"/*.proto;
done

# feast-spark-api
python -m grpc_tools.protoc -I. --python_out="${OUT_DIR}" --mypy_out="${OUT_DIR}" --grpc_python_out="${OUT_DIR}" feast_spark/api/*.proto;
