FROM python:3.7-slim-buster

ENV PROTOS_PACKAGE_PATH "/local/caraml-store-protobuf"

ADD utils/compile_protos.sh .
ADD requirements-ci.txt .

RUN pip install -r requirements-ci.txt

CMD /bin/bash compile_protos.sh ${PROTOS_PACKAGE_PATH}
