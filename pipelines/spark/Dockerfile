FROM bitnami/spark:3.5.1
USER root
RUN mkdir -p /opt/spark/workdir
WORKDIR /opt/spark/workdir
# Add commonly used connectors (Scala 2.12)
ENV SPARK_JARS=/opt/bitnami/spark/jars
RUN curl -L -o ${SPARK_JARS}/delta-spark_2.12-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -L -o ${SPARK_JARS}/spark-sql-kafka-0-10_2.12-3.5.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o ${SPARK_JARS}/kafka-clients-3.7.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar && \
    curl -L -o ${SPARK_JARS}/delta-storage-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
USER 1001
