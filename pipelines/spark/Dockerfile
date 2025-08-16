FROM bitnami/spark:3.5.1

USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/workdir
WORKDIR /opt/spark/workdir

ENV SPARK_JARS=/opt/bitnami/spark/jars

RUN set -eux; \
    rm -f ${SPARK_JARS}/spark-sql-kafka-0-10_2.12-*.jar || true; \
    rm -f ${SPARK_JARS}/spark-token-provider-kafka-0-10_2.12-*.jar || true; \
    rm -f ${SPARK_JARS}/kafka-clients-*.jar || true; \
    rm -f ${SPARK_JARS}/delta-spark_2.12-*.jar || true; \
    rm -f ${SPARK_JARS}/delta-storage-*.jar || true

RUN set -eux; \
    curl -fL -o ${SPARK_JARS}/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar; \
    curl -fL -o ${SPARK_JARS}/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar; \
    curl -fL -o ${SPARK_JARS}/kafka-clients-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar; \
    curl -fL -o ${SPARK_JARS}/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar; \
    curl -fL -o ${SPARK_JARS}/delta-storage-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar; \
    curl -fL -o ${SPARK_JARS}/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

USER 1001
