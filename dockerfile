FROM apache/airflow:3.1.6

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk procps wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark client (not full cluster)
ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

# 3. Set Environment Variables for JDK 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${PATH}:${SPARK_HOME}/bin"

USER airflow

# 4. Install python dependencies (these are small, so pip is fine)
RUN pip install --no-cache-dir \
    pyspark==3.5.2 \
    apache-airflow-providers-apache-spark==4.8.0 \
    delta-spark==3.2.0