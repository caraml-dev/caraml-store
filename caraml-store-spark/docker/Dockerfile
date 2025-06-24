FROM --platform=linux/amd64 apache/spark-py:v3.1.3

ARG GCS_CONNECTOR_VERSION=2.2.5
ARG BQ_CONNECTOR_VERSION=0.27.1
ARG ODPS_JDBC_CONNECTOR=3.8.2
ARG HADOOP_ALIYUN_VERSION=3.3.2
ARG ALIYUN_SDK_OSS_VERSION=3.18.2
ARG JDOM_VERSION=1.1

USER root
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-${GCS_CONNECTOR_VERSION}.jar /opt/spark/jars
ADD https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/${BQ_CONNECTOR_VERSION}/spark-bigquery-with-dependencies_2.12-${BQ_CONNECTOR_VERSION}.jar /opt/spark/jars
ADD https://github.com/aliyun/aliyun-odps-jdbc/releases/download/v${ODPS_JDBC_CONNECTOR}/odps-jdbc-${ODPS_JDBC_CONNECTOR}-jar-with-dependencies.jar /opt/spark/jars
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aliyun/${HADOOP_ALIYUN_VERSION}/hadoop-aliyun-${HADOOP_ALIYUN_VERSION}.jar /opt/spark/jars
ADD https://repo1.maven.org/maven2/com/aliyun/oss/aliyun-sdk-oss/${ALIYUN_SDK_OSS_VERSION}/aliyun-sdk-oss-${ALIYUN_SDK_OSS_VERSION}.jar /opt/spark/jars
ADD https://repo1.maven.org/maven2/org/jdom/jdom/${JDOM_VERSION}/jdom-${JDOM_VERSION}.jar /opt/spark/jars

RUN pip install Jinja2==3.1.2
# For logging to /dev/termination-log
RUN mkdir -p /dev

ADD caraml-spark-application-with-dependencies.jar /opt/spark/jars
ADD custom-dialect.jar /opt/spark/jars
ADD templates /opt/spark/work-dir/
ADD historical_feature_retrieval_job.py /opt/spark/work-dir