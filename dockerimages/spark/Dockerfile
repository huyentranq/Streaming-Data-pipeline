FROM docker.io/bitnami/spark:3.4.0

USER root
RUN apt-get update && apt-get install -y curl

ENV JAVA_HOME=/opt/bitnami/java
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> $SPARK_HOME/conf/spark-env.sh \
 && echo 'export PATH=$JAVA_HOME/bin:$PATH'    >> $SPARK_HOME/conf/spark-env.sh

WORKDIR /tmp/jars
# --- Kafka connector & token-provider ---
RUN curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar \
 && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.0/spark-token-provider-kafka-0-10_2.12-3.4.0.jar \
# --- Delta + S3A ---
 && curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
 && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar \
 && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar \
 && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.681/aws-java-sdk-bundle-1.12.681.jar \
 && curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
# --- Add PostgreSQL JDBC driver ---
 && curl -O https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \

 
 && mv *.jar $SPARK_HOME/jars/ \
 && rm -rf /tmp/jars

RUN pip install --no-cache-dir delta-spark==2.4.0 minio
RUN chmod -R 0775 /opt/bitnami/spark/conf
USER 1001
WORKDIR $SPARK_HOME
